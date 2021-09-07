use flate2::read::MultiGzDecoder;
use std::io::Write;
use std::{
    fs::File,
    io::{BufReader, Read},
    path::Path,
    path::PathBuf,
};

#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Custom(String),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}
pub fn decompress(file: &Path) -> Result<PathBuf, Error> {
    unimplemented!();

    // //open file handler
    // let f = File::open(file)?;

    // let mut out = String::new();

    // // buffer will contain gzipped content
    // let buf = BufReader::new(f);
    // let mut gzd = MultiGzDecoder::new(buf);

    // gzd.read_to_string(&mut out)?;

    // debug!("decompressed {:?}", file);
    // // change .../result/0.txt.gz into .../data/0.txt
    // let stem = file
    //     .file_stem()
    //     .ok_or_else(|| Error::Custom("no file name".to_string()))?
    //     .to_str()
    //     .ok_or_else(|| Error::Custom("file name is not valid unicode".to_string()))?;
    // // let dest_folder: PathBuf = [dest_folder, &PathBuf::from(stem)].iter().collect();
    // let dest_folder: PathBuf = PathBuf::from(format!("data/{}", stem));
    // debug!("will store into {:?}", dest_folder);

    // out_file.write_all(out.as_bytes())?;
    // // (out, &mut out_file)?;

    // // out_file
    // Ok(file.to_path_buf())
}
