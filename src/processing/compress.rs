use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use flate2::{write::GzEncoder, Compression};
use rayon::prelude::*;

use crate::error::Error;
use log::{error, info};
// use flate2::Compresion;

// fn compress_lang(dst: &Path, lang: &'static str, reader: Reader) {
//     let mut filepath = dst.clone();
//     filepath.push(format!("{}.txt.gz", )
//     let gz_writer = GzEncoder::new();
// }
pub fn compress_corpus(src: &Path, dst: &Path) -> Result<(), Error> {
    let files_to_compress: Result<Vec<_>, std::io::Error> = std::fs::read_dir(src)?.collect();
    let files_to_compress: Vec<PathBuf> =
        files_to_compress?.into_iter().map(|x| x.path()).collect();
    let files_to_compress = files_to_compress.into_par_iter();
    let errors: Vec<Error> = files_to_compress
        .filter_map(|filepath| match compress_file(&filepath, dst) {
            Ok(_) => None,
            Err(e) => Some(e),
        })
        .collect();

    if !errors.is_empty() {
        for error in errors {
            error!("{:?}", error);
            println!("{:?}", error);
        }
    }
    Ok(())
}

fn compress_file(path: &Path, dst: &Path) -> Result<(), Error> {
    let src = File::open(path)?;
    let b = BufReader::new(src);

    // gen filename
    let filename = path.file_name().unwrap();
    let mut dst: PathBuf = [dst.as_os_str(), filename].iter().collect();
    let extension = String::from(dst.extension().unwrap().to_str().unwrap());
    dst.set_extension(extension + ".gz");

    info!("compressing {:?} to {:?}", path, dst);

    let dest_file = File::create(dst)?;
    let mut enc = GzEncoder::new(dest_file, Compression::default());
    for line in b.lines() {
        let line = line?;
        enc.write_all(&line.as_bytes())?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::compress_corpus;

    //TODO
}
