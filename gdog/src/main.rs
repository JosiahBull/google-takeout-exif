use std::{env, path::Path};

#[tokio::main]
async fn main() {
    // input dir is first arg, output dir is second arg
    let args: Vec<String> = env::args().collect();
    let input_dir = &args[1];
    let output_dir = &args[2];

    // validate input and output dirs
    if !Path::new(input_dir).exists() {
        panic!("Input directory does not exist");
    }
    if !Path::new(output_dir).exists() {
        panic!("Output directory does not exist");
    }

    let mut processor = g_takeout_processor::Processor::new(input_dir, output_dir);

    // load and find all photos
    processor.load_files().unwrap();

    // generate the destination path for each file
    processor.generate_destination_paths().unwrap();

    // remove duplicate photos
    processor.remove_duplicates().await.unwrap();

    // // copy the files to the destination path
    processor.copy_files().unwrap();

    // // use exif-tool to copy exif data into the file from the source .json file
    processor.apply_exif().await.unwrap();

    // // switch file formats where appropriate
    // processor.rename();

    // print as json using serde_json
    println!("{}", serde_json::to_string_pretty(&processor).unwrap());
}
