use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    thread::current,
};

use flate2::bufread::GzDecoder;
use itertools::Itertools;
// use libflate::gzip::Decoder as GzipReader;
use libflate::gzip::MultiDecoder as GzipReader;
use std::io::BufRead;
use std::path::PathBuf;
use warc::header::WarcHeader;
use warc::WarcReader;

// from warc code
const MB: usize = 1_048_576;

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Warc(warc::Error),
    Custom(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

/// strips warcs (for now)
/// should/will return an iterator over record
/// in order to be easily pipelined.
pub fn strip_warc(path: &PathBuf) -> Result<(), Error> {
    let gzip_file = File::open(path)?;
    let gzip_stream = GzipReader::new(gzip_file)?;

    // we use a different reader from the default one in the warc crate to
    // manage multipart gzipped content.
    let reader = BufReader::with_capacity(10 * MB, gzip_stream);

    let gzd = WarcReader::new(reader);
    for record in gzd {
        match record {
            Ok(r) => println!("{:?}", String::from_utf8_lossy(&r.body)),
            Err(e) => eprintln!("{:?}", e),
        }
    }
    Ok(())
}
