use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
};

use chrono::{DateTime, Local, LocalResult, NaiveDateTime, NaiveTime, TimeZone};
use fuzzywuzzy::{fuzz, process::extract_one, utils};
use rayon::prelude::{IntoParallelRefMutIterator, ParallelIterator};
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_256};
use tokio::io::AsyncReadExt;

const IGNORED_TYPES: &[&str] = &["html"];
const IGNORED_FILES: &[&str] = &[
    "metadata.json",
    "shared_album_comments.json",
    "user-generated-memory-titles.json",
    "print-subscriptions.json",
];

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
enum DestLocation {
    Shared,
    General,
    Albums,
}

#[derive(Debug, Serialize, Deserialize, Clone, Copy)]
pub enum MatchSource {
    NoMatch,
    JsonFile,
    FileName,
    DirectoryName,
    FuzzyMatch { score: u8 },
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MediaFile {
    media_path: PathBuf,
    destination_path: Option<PathBuf>,
    destination_type: Option<DestLocation>,
    json_path: Option<PathBuf>,
    media_creation_date: Option<DateTime<Local>>,
    match_source: MatchSource,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Processor<'a> {
    pub takeout_directory: &'a str,
    pub output_directory: &'a str,
    media_files: Vec<MediaFile>,
    json_files: HashSet<PathBuf>,
}

/// Upload and solving process for google takeout import:
/// 1. Find all media/json pairs in the takeout directory and match them together
/// 2. Use a fuzzy matching algorithm to find missing pairs - getting user confirmation for each and then renaming
/// 3. Iterate through pairs, rename/convert files that have the incorrect file format
/// 4. Use exiftool to extract information from the .json file and apply it to the media file (subprocess)
/// 5. Use directory and filenames to add exif information to files which do not have it
/// 6. Move folders around to valid subfolders to prepare for upload
/// 7. Remove duplicates (e.g. in order of preference, e.g. nuke shared folders before anything else)

fn try_parse_8_char_date(input: &str) -> Option<DateTime<Local>> {
    // if we see  8 digits in a row, that's probably a date in the format YYYYMMDD
    let mut date_string = String::new();
    for c in input.chars() {
        if c.is_ascii_digit() {
            date_string.push(c);
            if date_string.len() == 8 {
                break;
            }
        } else {
            date_string.clear();
        }
    }

    if date_string.len() == 8 {
        let year = date_string[0..4].parse::<i32>().unwrap();
        let month = date_string[4..6].parse::<u32>().unwrap();
        let day = date_string[6..8].parse::<u32>().unwrap();

        // check that this is a valid date
        if let Some(date) = chrono::NaiveDate::from_ymd_opt(year, month, day) {
            let date_time = NaiveDateTime::new(date, NaiveTime::from_hms_opt(0, 0, 0).unwrap());
            if let LocalResult::Single(local) = Local.from_local_datetime(&date_time) {
                return Some(local);
            }
        }
    }
    None
}

fn json_path_from_media_path(media_path: &Path) -> Vec<PathBuf> {
    // 2. If the file has ` (x)` where x is a number appended to the end:
    //  e.g. take the filename, strip the last (2+size_of_int_in_chars) from the filestem
    //  readd the file ext
    //  Append ` (x).json`
    let mut options = vec![];
    let mut json_filename = format!("{}.json", media_path.display());
    options.push(json_filename);
    let file_stem = media_path.file_stem().unwrap().to_str().unwrap();
    if file_stem.ends_with(')') {
        // extract the string between the two brackets (X)
        let mut extracted_string = String::new();
        for c in file_stem.chars().rev().skip(1) {
            if c == '(' {
                break;
            }
            extracted_string.push(c);
        }

        //flip string
        extracted_string = extracted_string.chars().rev().collect();

        // if the extracted string is NOT a number, break
        if let Ok(number) = extracted_string.parse::<u32>() {
            let file_ext = media_path.extension().unwrap().to_str().unwrap();
            let file_parent = media_path.parent().unwrap();
            json_filename = file_stem[..file_stem.len() - extracted_string.len() - 2].to_string();
            json_filename = file_parent
                .join(format!("{}.{}({}).json", json_filename, file_ext, number))
                .to_str()
                .unwrap()
                .to_string();
            options.insert(0, json_filename);
        }
    }

    for option in options.clone() {
        // 3. If the file is of type heic, the json file has NO extension, otherwise it's as usual:
        //  - heic: filename.heic -> filename.json
        //  - non-heic: filename.jpg -> filename.jpg.json
        if media_path.extension().unwrap().to_ascii_lowercase() == "heic" {
            options.push(option.replace(".heic.json", ".json"));
            options.push(option.replace(".HEIC.json", ".json"));
        }

        // sometimes for jpg/png, the json file is .p.json or .j.json
        if media_path.extension().unwrap().to_ascii_lowercase() == "jpg" {
            // try pushing the same thing with numbers 1-20 appended
            for i in 1..1000 {
                if option.contains(format!("({})", i).as_str()) {
                    // create a base without the .png.json and whatever (x) is appended
                    let base = option.replace(".jpg.json", "").replace(".JPG.json", "");
                    let length_in_chars = i.to_string().len();
                    let base = base[..base.len() - length_in_chars - 2].to_string();

                    let new_option = format!("{}.j({}).json", base, i);
                    options.push(new_option);
                }
            }

            let new_options = option
                .replace(".jpg.json", ".j.json")
                .replace(".JPG.json", ".j.json");
            options.push(new_options);
        }
        if media_path.extension().unwrap().to_ascii_lowercase() == "png" {
            // try pushing the same thing with numbers 1-20 appended
            for i in 1..2000 {
                if option.contains(format!("({})", i).as_str()) {
                    // create a base without the .png.json and whatever (x) is appended
                    let base = option.replace(".png.json", "").replace(".PNG.json", "");
                    let length_in_chars = i.to_string().len();
                    let base = base[..base.len() - length_in_chars - 2].to_string();

                    let new_option = format!("{}.p({}).json", base, i);
                    options.push(new_option);
                }
            }

            let new_option = option
                .replace(".png.json", ".p.json")
                .replace(".PNG.json", ".p.json");
            options.push(new_option);
        }
    }

    // if the file contains -edited try to remove it
    for option in options.clone() {
        options.push(option.replace("-edited", ""));
    }

    // sometimes it's ..json instead of .json
    for option in options.clone() {
        options.push(option.replace("..json", ".json"));
        options.push(option.replace("..", "."));
    }

    // try removing the last char of the filestem
    for option in options.clone() {
        let file_stem = Path::new(&option).file_stem().unwrap().to_str().unwrap();
        let file_parent = Path::new(&option).parent().unwrap();
        for i in 0..7 {
            if file_stem.len() <= i {
                break;
            }

            // if the char is a ) or (, break
            if file_stem.chars().rev().nth(i).unwrap() == ')' {
                break;
            }

            if let Some(res) = file_parent
                .join(format!(
                    "{}.json",
                    &file_stem
                        .chars()
                        .take(file_stem.len() - i)
                        .collect::<String>()
                ))
                .to_str()
            {
                options.push(res.to_string());
            }
        }
    }

    options.iter().map(PathBuf::from).collect()
}

impl<'a> Processor<'_> {
    pub fn new(takeout_directory: &'a str, output_directory: &'a str) -> Processor<'a> {
        Processor {
            takeout_directory,
            output_directory,
            media_files: Vec::new(),
            json_files: HashSet::new(),
        }
    }

    /// recursively search through the takeout_directory, and find all media files/json files - load them into the processor
    fn search_directory_recur(&mut self, path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
        for file in std::fs::read_dir(path)? {
            let file = file?;

            if file.file_type()?.is_dir() {
                self.search_directory_recur(file.path())?;
                continue;
            }

            // skip if file is in IGNORED_FILES
            let file_name = file.file_name();
            let file_name = file_name.to_str().unwrap();
            if IGNORED_FILES.contains(&file_name.to_lowercase().as_ref()) {
                continue;
            }

            // skip if ext is in IGNORED_TYPES
            if let Some(file_ext) = file.path().extension() {
                if IGNORED_TYPES.contains(&file_ext.to_str().unwrap().to_lowercase().as_ref()) {
                    continue;
                }
            }

            let file_path = file.path();
            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if file_name.ends_with(".json") {
                self.json_files.insert(file_path);
            } else {
                self.media_files.push(MediaFile {
                    media_path: file_path,
                    json_path: None,
                    destination_path: None,
                    destination_type: None,
                    media_creation_date: None,
                    match_source: MatchSource::NoMatch,
                });
            }
        }

        Ok(())
    }

    /// for each media file which does NOT have a json file, try to pull date information from the filename, or failing that, the pathname
    fn find_date_time_from_filename(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        for file in self.media_files.iter_mut() {
            if !matches!(file.match_source, MatchSource::NoMatch) {
                continue;
            }

            let mut file_name = file
                .media_path
                .file_stem()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();

            // replace (x) with nothing
            for x in 0..5 {
                file_name = file_name.replace(&format!("({})", x), "");
            }
            for replace_str in ["edited", "IMG", "VID", "JPEG", "EFFECTS"] {
                for accent in ["-", "_"] {
                    file_name = file_name.replace(&format!("{}{}", accent, replace_str), "");
                    file_name = file_name.replace(&format!("{}{}", replace_str, accent), "");
                }
            }

            // if the filename is less than 6 characters long, it's not a valid date
            if file_name.len() < 8 {
                continue;
            }

            // try to parse YYYYMMDD formats from the filename:
            if let Some(date) = try_parse_8_char_date(&file_name) {
                file.media_creation_date = Some(date);
                file.match_source = MatchSource::FileName;
                continue;
            }

            // try to parse YYYY-MM-DD and YYYY_MM_DD formats from the filename:
            for accent in ["-", "_", " "] {
                if let Some(date) = try_parse_8_char_date(&file_name.replace(accent, "")) {
                    file.media_creation_date = Some(date);
                    file.match_source = MatchSource::FileName;
                    continue;
                }
            }
        }
        Ok(())
    }

    /// for each media file which does NOT have a json file or date information attached, try to fuzzy match the filename to a json file
    fn fuzzy_match_filenames(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let mut total_count = 0;
        let count = AtomicUsize::new(0);
        for file in self.media_files.iter() {
            if matches!(file.match_source, MatchSource::NoMatch) {
                total_count += 1;
            }
        }

        let directory_map: RwLock<HashMap<PathBuf, Arc<Vec<String>>>> = RwLock::new(HashMap::new());

        let new_media_files = RwLock::new(self.json_files.clone());
        self.media_files
            .par_iter_mut()
            .filter(|i| matches!(i.match_source, MatchSource::NoMatch))
            .for_each(|file| {
                let file_name = file.media_path.file_name().unwrap().to_str().unwrap();

                // load possible matches ALL json files in the same parent directory thare aren't in the IGNORED_FILES list
                let parent_dir = file.media_path.parent().unwrap();

                let contains_key = directory_map.read().unwrap().contains_key(parent_dir);
                if !contains_key {
                    let mut local_possible_matches = Vec::with_capacity(self.json_files.len());
                    for file in std::fs::read_dir(parent_dir).unwrap() {
                        let file = file.unwrap();
                        if file.file_type().unwrap().is_dir() {
                            continue;
                        }
                        let file_name = file.file_name();
                        if IGNORED_FILES
                            .contains(&file_name.to_str().unwrap().to_lowercase().as_ref())
                        {
                            continue;
                        }
                        if let Some(file_ext) = file.path().extension() {
                            if IGNORED_TYPES
                                .contains(&file_ext.to_str().unwrap().to_lowercase().as_ref())
                            {
                                continue;
                            }
                        } else {
                            continue;
                        }
                        if file_name.to_str().unwrap().ends_with(".json") {
                            local_possible_matches.push(file_name.to_str().unwrap().to_owned());
                        }
                    }
                    directory_map
                        .write()
                        .unwrap()
                        .insert(parent_dir.to_owned(), Arc::new(local_possible_matches));
                }

                let possible_matches = {
                    let reader = directory_map.read().unwrap();
                    reader.get(parent_dir).unwrap().clone()
                };

                let result = extract_one(
                    file_name,
                    possible_matches.as_ref(),
                    utils::full_process,
                    fuzz::wratio,
                    90,
                );

                if let Some((json_name, score)) = result {
                    // find item in larger array, using both the expected parent directory, and the json file name
                    let true_name = file.media_path.parent().unwrap().join(json_name);
                    let res = new_media_files.read().unwrap().contains(&true_name);
                    if res {
                        new_media_files.write().unwrap().remove(&true_name);
                    }
                    println!(
                        "[{}/{}] Found match score[{}]: \n{}\n{}",
                        count.fetch_add(1, Ordering::Relaxed),
                        total_count,
                        score,
                        true_name.display(),
                        file.media_path.display()
                    );
                    file.json_path = Some(true_name);
                    file.match_source = MatchSource::FuzzyMatch { score };
                } else {
                    println!(
                        "[{}/{}] No match found for:\n{}",
                        count.fetch_add(1, Ordering::Relaxed),
                        total_count,
                        file.media_path.display()
                    );
                }
            });

        self.json_files = new_media_files.into_inner().unwrap();

        Ok(())
    }

    pub fn match_json_files_to_media_files(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // To find teh json file of a given file, follow the steps:
        // 0. If the file is in the exclusion HashSet, skip it.
        // 1. If the file is of type json, skip it.
        // 2. If the file has ` (x)` where x is a number appended to the end:
        //  e.g. take the filename, strip the last (2+size_of_int_in_chars) from the filestem
        //  readd the file ext
        //  Append ` (x).json`
        // 3. If the file is of type heic, the json file has NO extension, otherwise it's as usual:
        //  - heic: filename.heic -> filename.json
        //  - non-heic: filename.jpg -> filename.jpg.josn
        // 4. Attempt to find the json file
        // 5. If json file found AND input file is .heic, add item to exclusion hashset for extra .mp4 (if present)
        let mut exclusion = HashMap::new();
        for file in self.media_files.iter_mut() {
            if !matches!(file.match_source, MatchSource::NoMatch) {
                continue;
            }

            // If file is in the exclusion hashset, skip it
            if exclusion.contains_key(&file.media_path) {
                continue;
            }

            // If file is a json file, skip it
            if file.media_path.extension().is_none()
                || file.media_path.extension().unwrap().to_ascii_lowercase() == "json"
            {
                continue;
            }

            // 4. Attempt to find the json file
            let potential_paths = json_path_from_media_path(&file.media_path);

            for potential_path in potential_paths {
                // remove json file from json_files HashSet
                if self.json_files.contains(&potential_path) {
                    self.json_files.remove(&potential_path);
                }

                if potential_path.exists() {
                    file.json_path = Some(potential_path.to_path_buf());
                    file.match_source = MatchSource::JsonFile;

                    // 5. If json file found AND input file is .heic, add item to exclusion hashset for extra .mp4 (if present)
                    // sometimes this also applies to .JPG files?
                    if file.media_path.extension().unwrap().to_ascii_lowercase() == "heic"
                        || file.media_path.extension().unwrap().to_ascii_lowercase() == "jpg"
                    {
                        let mp4_path = file.media_path.with_extension("MP4");
                        if mp4_path.exists() {
                            exclusion.insert(mp4_path, file.json_path.clone());
                        }
                        let mp4_path = file.media_path.with_extension("mp4");
                        if mp4_path.exists() {
                            exclusion.insert(mp4_path, file.json_path.clone());
                        }
                    }
                    break;
                }
            }
        }

        for (excluded, json_file) in exclusion.into_iter() {
            if let Some(file) = self
                .media_files
                .iter_mut()
                .find(|f| f.media_path == *excluded)
            {
                if !matches!(file.match_source, MatchSource::NoMatch) {
                    continue;
                }

                file.json_path = json_file;
                file.match_source = MatchSource::JsonFile;
            }
        }

        Ok(())
    }

    pub fn load_files(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        self.search_directory_recur(self.takeout_directory.into())?;

        self.match_json_files_to_media_files()?;

        self.fuzzy_match_filenames()?;

        // Try to load date/times from filenames
        self.find_date_time_from_filename()?;

        println!("Number of unmatched json files: {}", self.json_files.len());
        println!(
            "Number of unmatched media files: {}",
            self.media_files
                .iter()
                .filter(|x| x.json_path.is_none() && x.media_creation_date.is_none())
                .count()
        );

        let mut no_match = 0;
        let mut json_file = 0;
        let mut file_name = 0;
        let mut directory_name = 0;
        let mut fuzzy_match = 0;
        for file in self.media_files.iter() {
            match file.match_source {
                MatchSource::NoMatch => no_match += 1,
                MatchSource::JsonFile => json_file += 1,
                MatchSource::FileName => file_name += 1,
                MatchSource::DirectoryName => directory_name += 1,
                MatchSource::FuzzyMatch { .. } => fuzzy_match += 1,
            }
        }
        println!("Matched by json file: {}", json_file);
        println!("Matched by file name: {}", file_name);
        println!("Matched by directory name: {}", directory_name);
        println!("Matched by fuzzy match: {}", fuzzy_match);
        println!("No match: {}", no_match);

        // print all unmatched json files
        for file in self.media_files.iter() {
            if file.json_path.is_none()
                && file.media_creation_date.is_none()
                && file.media_path.file_stem().unwrap().len() > 8
            {
                println!("Could not find date/time for file: {:?}", file.media_path);
            }
        }

        for file in self.json_files.iter() {
            println!("Could not find match for json file: {:?}", file);
        }

        Ok(())
    }

    pub fn generate_destination_paths(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // each file can go into one of three directories:
        // 1. General Photos
        // 2. Albums
        // 3. Shared Albums

        // Based on their path, they should be moved in to one of these three directories.
        // Photos in a folder named "Archive" -> General Photos
        // Photos in a folder named "Photos from YYYY" where YYYY is a year -> General Photos
        // Photos in a folder named "Untitled" or "Untitled(x)" where x is an integer -> Shared Albums
        // Photos in any other folders -> Album (preserve folder structure)
        let mut extension_mismatch_count = 0;

        // Iterate through all files and sort based on their path
        for media_file in self.media_files.iter_mut() {
            let file_path = &media_file.media_path;
            let file_parent = file_path.parent().unwrap();
            let file_parent_name = file_parent.file_name().unwrap().to_str().unwrap();

            let general_photos: PathBuf =
                PathBuf::from(format!("{}/general", self.output_directory));
            let albums: PathBuf = PathBuf::from(format!("{}/albums", self.output_directory));
            let shared_albums: PathBuf =
                PathBuf::from(format!("{}/shared/shared", self.output_directory));

            match file_parent_name {
                "Archive" => {
                    media_file.destination_path =
                        Some(general_photos.join(file_path.file_name().unwrap()));
                    media_file.destination_type = Some(DestLocation::General);
                }
                x if x.starts_with("Photos from ") && x.len() == "Photos from 2000".len() => {
                    media_file.destination_path =
                        Some(general_photos.join(file_path.file_name().unwrap()));
                    media_file.destination_type = Some(DestLocation::General);
                }
                "Untitled" => {
                    media_file.destination_path =
                        Some(shared_albums.join(file_path.file_name().unwrap()));
                    media_file.destination_type = Some(DestLocation::Shared);
                }
                x if x.starts_with("Untitled(") => {
                    media_file.destination_path =
                        Some(shared_albums.join(file_path.file_name().unwrap()));
                    media_file.destination_type = Some(DestLocation::Shared);
                }
                _ => {
                    // include only the parent directory and the file name
                    let mut destination_path = PathBuf::from(file_parent_name);
                    destination_path.push(file_path.file_name().unwrap());
                    media_file.destination_path = Some(albums.join(destination_path));
                    media_file.destination_type = Some(DestLocation::Albums);
                }
            }

            let dest_path = media_file.destination_path.as_ref().unwrap();

            // update the destination path with the correct extension
            // skip incompatible extensions
            if dest_path.extension().is_some() && (dest_path.extension().unwrap() == "MTS") {
                continue;
            }

            // use the unix "file" command to determine the file type if a file has no ext
            let file_type = Command::new("file")
                .arg(file_path)
                .output()
                .expect("failed to execute process");
            let file_type = String::from_utf8_lossy(&file_type.stdout);
            let file_type = file_type.to_string().to_ascii_lowercase();
            let file_type = file_type.split(':').into_iter().nth(1).unwrap();

            // println!("{}: {}", file_path.display(), file_type);

            let dest_file_ext = {
                if file_type.contains("png image data") {
                    "png"
                } else if file_type.contains("jpg image data")
                    || file_type.contains("jpeg image data")
                {
                    "jpg"
                } else if file_type.contains("gif image data") {
                    "gif"
                } else if file_type.contains("heic image data")
                    || file_type.contains("iso media, heif image hevc main")
                {
                    "heic"
                } else if file_type.contains("mp3 audio") {
                    "mp3"
                } else if file_type.contains("apple quicktime movie") {
                    "mov"
                } else if file_type.contains("mp4 video")
                    || file_type.contains("iso media, mp4 v")
                    || file_type.contains("iso media, mp4 base media v")
                    || file_type.contains("iso media, mpeg-4")
                    || file_type.contains("iso media, mpeg v")
                {
                    "mp4"
                } else if file_type.contains("mov video") {
                    "mov"
                } else if file_type.contains("3gp video") {
                    "3gp"
                } else if file_type.contains("tiff image data") {
                    "tiff"
                } else if file_type.contains("pc bitmap") {
                    "bmp"
                } else if file_type.contains("apple itunes video (.m4v)") {
                    "m4v"
                } else if file_type.contains("web/p image") {
                    "webp"
                } else if file_type.contains("microsoft asf") {
                    "asf"
                } else if file_type.contains("mpeg sequence") {
                    "mpeg"
                } else if file_type.contains("avi") {
                    "avi"
                } else if file_type.contains("canon cr2") {
                    "cr2"
                } else if file_type.trim() == "data"
                    || file_type.contains("ascii text")
                    || file_type.contains("canon ciff raw image data")
                {
                    // wtf is this? return the original extension
                    dest_path.extension().unwrap().to_str().unwrap()
                } else {
                    panic!(
                        "Unknown file type: `{}` while processing file `{}`",
                        file_type,
                        file_path.display()
                    );
                }
            };

            // if they aren't the same, increment a counter
            if dest_path.extension().is_none()
                || dest_path.extension().unwrap().to_ascii_lowercase() != dest_file_ext
            {
                extension_mismatch_count += 1;
                println!("Extension mismatch: {:?} -> {:?}", dest_path, dest_file_ext)
            }

            let mut new_dest_path = dest_path.clone();
            new_dest_path.set_extension(dest_file_ext);
            media_file.destination_path = Some(new_dest_path);
        }

        println!("Extension mismatch count: {}", extension_mismatch_count);

        // // iterate and print all filenames
        // for file in self.media_files.iter() {
        //     println!("File: {:?}", file.media_path);
        //     println!("Destination: {:?}", file.destination_path);
        //     println!("Date: {:?}", file.media_creation_date);
        //     println!("Json: {:?}", file.json_path);
        //     println!("Match Source: {:?}", file.match_source);
        //     println!();
        // }

        Ok(())
    }

    //TODO; multithreading is required for this function
    pub async fn remove_duplicates(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // remove duplicate files, using sha256 hash as a benchmark for what is and isn't a duplicate file
        // we want to remove files in a specific priority to preserve file structure in albums
        // 1. Files in the "general" directory are removed first
        // 2. Files in the "shared" directory are removed next
        // 3. Files in the "albums" directory are never removed.

        println!("Removing duplicate files");

        let mut counter = 0;
        let total_files = self.media_files.len();
        let mut files: HashMap<String, Vec<&MediaFile>> = HashMap::new();
        for chunk in self.media_files.chunks(1024) {
            let mut futures = Vec::with_capacity(1024);

            for media_file in chunk {
                let num = counter;
                counter += 1;

                futures.push(async move {
                    let mut hasher = Sha3_256::new();
                    let mut file = tokio::fs::File::open(&media_file.media_path).await.unwrap();
                    let mut buf = [0; 1024];
                    loop {
                        let n = file.read(&mut buf).await.unwrap();
                        if n == 0 {
                            break;
                        }
                        hasher.update(&buf[..n]);
                    }

                    println!("Hashing file {}/{}", num, total_files);

                    let hash = hasher.finalize_reset();
                    format!("{:x}", hash)
                });
            }

            let hashes = futures::future::join_all(futures).await;

            for (i, hash) in hashes.iter().enumerate() {
                let media_file = &self.media_files[i];
                let files = files.entry(hash.to_string()).or_default();
                files.push(media_file);
            }
        }

        files.retain(|_, v| v.len() > 1);

        // for any arrays > 1 element, we need to remove the duplicates
        for (_, files) in files.iter_mut() {
            // sort the files by their destination path
            // albums -> shared -> general
            files.sort_by(|a, b| {
                match (a.destination_type.unwrap(), b.destination_type.unwrap()) {
                    (DestLocation::Albums, _) => std::cmp::Ordering::Less,
                    (_, DestLocation::Albums) => std::cmp::Ordering::Greater,
                    (DestLocation::Shared, _) => std::cmp::Ordering::Less,
                    (_, DestLocation::Shared) => std::cmp::Ordering::Greater,
                    _ => std::cmp::Ordering::Equal,
                }
            });

            // remove the first item
            files.remove(0);
        }

        // remove all files from the files array, comparing on the source path
        let to_be_removed: HashSet<String> = files
            .iter()
            .flat_map(|(_, v)| v.iter())
            .map(|f| f.media_path.to_str().unwrap().to_string())
            .collect();
        println!("Removing {} files", to_be_removed.len());
        self.media_files
            .retain(|f| !to_be_removed.contains(&f.media_path.to_str().unwrap().to_string()));

        Ok(())
    }

    pub fn copy_files(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        println!("Copying files");
        // for each media file, copy it to it's desired ending location
        let mut col_rectifier = 0;
        let total_files = self.media_files.len();

        for (i, file) in self.media_files.iter_mut().enumerate() {
            println!("Copying file [{}/{}]: {}", i, total_files, file.media_path.display());

            let mut destination_path = file.destination_path.as_ref().unwrap();
            let media_path = &file.media_path;

            // create the directory if it doesn't exist
            if let Some(parent) = destination_path.parent() {
                if !parent.exists() {
                    std::fs::create_dir_all(parent)?;
                }
            }

            // check if the file already exists
            if destination_path.exists() {
                // if it does, then we need to change the filename
                let mut new_dest_path = destination_path.clone();
                new_dest_path.set_file_name(format!(
                    "{}_{}.{}",
                    new_dest_path.file_stem().unwrap().to_str().unwrap(),
                    col_rectifier,
                    new_dest_path.extension().unwrap().to_str().unwrap()
                ));
                col_rectifier += 1;

                file.destination_path = Some(new_dest_path);
                destination_path = file.destination_path.as_ref().unwrap();
            }

            // copy the file
            std::fs::copy(media_path, destination_path)?;
        }

        Ok(())
    }

    pub async fn apply_exif(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        // use exiftool to apply the date to the file using JSON where possible, otherwise use the one set in the file
        // or ask for manual intervention

        let counter = Arc::new(AtomicUsize::new(1));
        let total_media_files = self.media_files.len();
        for chunk in self.media_files.chunks(1024) {
            let mut futures = Vec::with_capacity(1024);

            for media_file in chunk.iter() {
                let counter = counter.clone();
                futures.push(async move {

                // if JSON
                if media_file.json_path.is_some() {
                    // COPIED FROM: https://github.com/kaytat/exiftool-scripts-for-takeout
                    let process = tokio::process::Command::new("exiftool")
                        .args([
                            // Attempt to modify media ONLY if the info doesn't already exist
                            "-if",
                            "($Filetype eq \"MP4\" and not $quicktime:TrackCreateDate) or ($Filetype eq \"MP4\" and $quicktime:TrackCreateDate eq \"0000:00:00 00:00:00\") or ($Filetype eq \"JPEG\" and not $exif:DateTimeOriginal) or ($Filetype eq \"PNG\" and not $PNG:CreationTime)",

                            "-tagsfromfile",
                            format!("{}", media_file.json_path.as_ref().unwrap().display()).as_str(),

                            // exif for regular jpg
                            "-AllDates<${PhotoTakenTimeTimestamp;$_=ConvertUnixTime($_,1)}",
                            // png specific
                            "-XMP-Exif:DateTimeOriginal<${PhotoTakenTimeTimestamp;$_=ConvertUnixTime($_,1)}",
                            "-PNG:CreationTime<${PhotoTakenTimeTimestamp;$_=ConvertUnixTime($_,1)}",
                            // Quicktime/mp4. Assume timestamp is in UTC.
                            "-QuickTime:TrackCreateDate<${PhotoTakenTimeTimestamp;$_=ConvertUnixTime($_,0)}",
                            "-QuickTime:TrackModifyDate<${PhotoTakenTimeTimestamp;$_=ConvertUnixTime($_,0)}",
                            "-QuickTime:MediaCreateDate<${PhotoTakenTimeTimestamp;$_=ConvertUnixTime($_,0)}",
                            "-QuickTime:MediaModifyDate<${PhotoTakenTimeTimestamp;$_=ConvertUnixTime($_,0)}",

                            // clobber everything
                            "-overwrite_original",

                            // add the target file
                            format!("{}", media_file.destination_path.as_ref().unwrap().display()).as_str(),
                        ])
                        // capture stdout and stderr
                        .stdout(Stdio::piped())
                        .stderr(Stdio::piped())
                        .output().await;

                    if let Err(e) = process {
                        println!("[{}/{}] Applying exif to {}... FAILURE! `{}`", counter.fetch_add(1, Ordering::Relaxed), total_media_files, media_file.destination_path.as_ref().unwrap().display(), e);
                        return;
                    }
                    let process = process.unwrap();

                    if process.status.success() {
                        println!("[{}/{}] Applying exif to {}... Success!", counter.fetch_add(1, Ordering::Relaxed), total_media_files, media_file.destination_path.as_ref().unwrap().display());
                    } else {
                        println!("[{}/{}] Applying exif to {}... FAILURE! `{}` `{}`", counter.fetch_add(1, Ordering::Relaxed), total_media_files, media_file.destination_path.as_ref().unwrap().display(),  String::from_utf8_lossy(&process.stderr).replace('\r', "").replace('\n', "  "), String::from_utf8_lossy(&process.stdout).replace('\r', "").replace('\n', "  "));

                    }

                    // read the json file
                    let json_file = tokio::fs::read_to_string(media_file.json_path.as_ref().unwrap()).await.unwrap();

                    // parse the json file
                    let json: serde_json::Value = serde_json::from_str(&json_file).unwrap();

                    //read creationTime.timestamp
                    let crt_timestamp = json["creationTime"]["timestamp"].as_str().unwrap();
                    // read photoLastModifiedTime.timestamp
                    let photo_timestamp = json["photoLastModifiedTime"]["timestamp"].as_str().unwrap();

                    // convert the epoch timestamps to DateTime
                    let crt_epoch = crt_timestamp.parse::<i64>().unwrap();
                    let crt_epoch = chrono::NaiveDateTime::from_timestamp_opt(crt_epoch, 0).unwrap();
                    let photo_epoch = photo_timestamp.parse::<i64>().unwrap();
                    let photo_epoch = chrono::NaiveDateTime::from_timestamp_opt(photo_epoch, 0).unwrap();


                    // select the earliest timestamp
                    let to_apply = {
                        if crt_epoch < photo_epoch {
                            crt_epoch
                        } else {
                            photo_epoch
                        }
                    };

                    // use the filetime crate to set the file's timestamp, in a blocking runtime
                    let dest_path = media_file.destination_path.clone().unwrap();
                    tokio::task::spawn_blocking(move || {
                        let file_time = filetime::FileTime::from_unix_time(to_apply.timestamp(), to_apply.timestamp_subsec_nanos());
                        filetime::set_file_times(dest_path, file_time, file_time).unwrap();
                    }).await.unwrap();
                } else {
                    println!("NO JSON FOUND!");
                    // print the media file and all information with it
                    println!("{:#?}", media_file);
                }
                })
            }

            futures::future::join_all(futures).await;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::json_path_from_media_path;

    #[test]
    fn test_json_path_brackets_at_end() {
        let media_path = PathBuf::from("/home/tester/images/my_bracket(1).png");
        let json_path = json_path_from_media_path(&media_path).remove(0);
        assert_eq!(
            json_path,
            PathBuf::from("/home/tester/images/my_bracket.png(1).json")
        );
    }

    #[test]
    fn test_json_path_brackets_invalid_at_end() {
        let media_path = PathBuf::from("/home/tester/images/my_bracket(ooga).png");
        let json_path = json_path_from_media_path(&media_path).remove(0);
        assert_eq!(
            json_path,
            PathBuf::from("/home/tester/images/my_bracket(ooga).png.json")
        );
    }

    #[test]
    fn test_heic_file_with_video() {
        let media_path = PathBuf::from(
            "/home/tester/images/66694115136__8679EE1A-E4B4-4D1B-B76C-510A6E58C.HEIC",
        );
        let json_path = json_path_from_media_path(&media_path).remove(0);
        assert_eq!(
            json_path,
            PathBuf::from(
                "/home/tester/images/66694115136__8679EE1A-E4B4-4D1B-B76C-510A6E58C.json"
            )
        );
    }

    #[test]
    fn test_heic_file_rough() {
        let media_path =
            PathBuf::from("/home/tester/images/Google Photos/Photos from 2020/IMG_2433.HEIC");
        let json_path = json_path_from_media_path(&media_path).remove(1);
        assert_eq!(
            json_path,
            PathBuf::from("/home/tester/images/Google Photos/Photos from 2020/IMG_2433.HEIC.json")
        );
    }

    #[test]
    fn test_json_path_brackets_at_end_double_digit() {
        let media_path = PathBuf::from("/home/tester/images/my_bracket(16).png");
        let json_path = json_path_from_media_path(&media_path).remove(0);
        assert_eq!(
            json_path,
            PathBuf::from("/home/tester/images/my_bracket.png(16).json")
        );
    }

    #[test]
    fn test_bracket_ordering() {
        let media_path = PathBuf::from("/home/josiah/Documents/g-takeout-processor/gdog/takeout/Google Photos/Photos from 2018/2018-06-17 01_54_22-13th June - OneNote 2016(1).png");
        let json_path = json_path_from_media_path(&media_path).remove(0);
        assert_eq!(
        json_path,
        PathBuf::from("/home/josiah/Documents/g-takeout-processor/gdog/takeout/Google Photos/Photos from 2018/2018-06-17 01_54_22-13th June - OneNote 2016.png(1).json")
    );
    }
}
