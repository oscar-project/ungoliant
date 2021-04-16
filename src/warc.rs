use std::{
    fs::File,
    io::{BufReader, BufWriter, Write},
    path::Path,
    thread::current,
};

use flate2::bufread::{GzDecoder, MultiGzDecoder};
use itertools::Itertools;
// use libflate::gzip::Decoder as GzipReader;
use libflate::gzip::MultiDecoder;
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

pub struct Wet<T> {
    reader: WarcReader<T>,
}

impl Wet<BufReader<MultiDecoder<File>>> {
    pub fn from_path_gzip<P: AsRef<Path>>(path: P) -> std::io::Result<Self> {
        let gzip_file = File::open(path)?;
        let gzip_stream = MultiDecoder::new(gzip_file)?;

        // we use a different reader from the default one in the warc crate to
        // manage multipart gzipped content.
        let bufreader = BufReader::with_capacity(100 * MB, gzip_stream);

        let reader = WarcReader::new(bufreader);

        Ok(Self { reader })
    }
}

impl<R: BufRead> Iterator for Wet<R> {
    type Item = Result<String, warc::Error>;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(n) = self.reader.next() {
            match n {
                Ok(record) => Some(Ok(String::from_utf8_lossy(&record.body).to_string())),
                Err(e) => Some(Err(e)),
            }
        } else {
            None
        }
    }
}
