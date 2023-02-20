# Google Takeout Exif For Immich

A Rust tool which will intake a Google Takeout archive and copy all files to a new directory, with exif data applied.

This tool was largely hacked together in a weekend for my needs, if you found it useful please let me know and I'll be happy to improve it by adding a proper CLI, documentation, and refactoring the code to be a lot more idiomatic.

The output directory will loop as follows:

```bash
> tree output
/output
/output/general/:photos # all photos that are not in an album or shared album
/output/shared/shared/:photos # all photos in shared albums are merged into a single directory
/output/albums/:albumname/:photos
```

These can then easily be uploaded to Immich using the following commands:
```bash
immich upload --key API_KEY --server SERVER_URL -d ./albums --album
immich upload --key API_KEY --server SERVER_URL -d ./shared --album
immich upload --key API_KEY --server SERVER_URL -d ./general
```

This tool is multithreaded, and does all of the following steps:
1. Matching up files to their JSON counterparts
2. Using exiftool internally to apply exif data to the files
3. Removing duplicate files
4. Correcting incorrect file extensions (if you use the compression setting in Google Photos this is almost certain to be required)
5. Adding identifiers to non-unique filenames
6. Copying files to the output directory

## Usage

```bash
cd gdog && cargo build --release
./target/release/gdog
```

```bash
USAGE:
    immich-exif-for-google-takeout <input> <output>
```