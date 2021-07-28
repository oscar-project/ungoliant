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

/// Compress a whole corpus using concurrently.
///
/// files in `src` will be kept (contrary to `gzip`'s behaviour).
///
/// Returns either a potentially empty vector of failed compressions, or an error related to directory reading/listing
pub fn compress_corpus(src: &Path, dst: &Path) -> Result<Vec<Error>, Error> {
    // There should be an easier way to do that.
    let files_to_compress: Result<Vec<_>, std::io::Error> = std::fs::read_dir(src)?.collect();
    let files_to_compress: Vec<PathBuf> =
        files_to_compress?.into_iter().map(|x| x.path()).collect();
    let files_to_compress = files_to_compress.into_par_iter();

    // construct vector of errors
    let errors: Vec<Error> = files_to_compress
        .filter_map(|filepath| compress_file(&filepath, dst).err())
        .collect();

    if !errors.is_empty() {
        for error in &errors {
            error!("{:?}", error);
        }
    };

    Ok(Vec::new())
}

/// compress a single file
fn compress_file(path: &Path, dst: &Path) -> Result<(), Error> {
    let src = File::open(path)?;
    let mut b = BufReader::new(src);

    // gen filename
    let filename = path.file_name().unwrap();
    let mut dst: PathBuf = [dst.as_os_str(), filename].iter().collect();
    let extension = String::from(dst.extension().unwrap().to_str().unwrap());
    dst.set_extension(extension + ".gz");

    info!("compressing {:?} to {:?}", path, dst);

    let dest_file = File::create(dst)?;
    let mut enc = GzEncoder::new(dest_file, Compression::default());

    let mut length = 1;
    while length > 0 {
        let buffer = b.fill_buf()?;
        enc.write_all(buffer)?;
        length = buffer.len();
        b.consume(length);
    }

    enc.try_finish()?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::compress_corpus;

    //TODO
}
