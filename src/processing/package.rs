use log::warn;
use rayon::prelude::*;
use std::fs::File;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use log::error;
use log::{debug, info};
use sha2::Digest;
use sha2::Sha256;

use crate::error::Error;
use crate::lang::LANG;
pub fn gen_checksum_file(src: &Path) -> Result<(), Error> {
    let mut hasher = Sha256::new();
    for f in std::fs::read_dir(src)? {
        let f = f?;
        println!("{:?}", get_hash(&f.path(), &mut hasher));
    }

    Ok(())
}
fn get_hash(f: &Path, hasher: &mut Sha256) -> Result<String, Error> {
    let mut f = File::open(f)?;
    let n = io::copy(&mut f, hasher)?;
    let result = format!("{:x}", hasher.finalize_reset());
    Ok(result)
}

fn put_in_lang_folder(filename: &Path, dst: &Path, lang: &'static str) -> Result<(), Error> {
    let mut dst = PathBuf::from(dst);
    dst.push(lang);
    let mut dst_txt = dst.clone();
    dst_txt.push(filename.file_name().unwrap());

    //create directory and move files
    // println!("[{}] creating {:?}", lang, dst);
    // println!("[{}] will copy txt  to {:?}", lang, dst_txt);

    //return error only if it's not an AlreadyExists error
    if let Err(e) = std::fs::create_dir(dst) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(Error::Io(e));
        }
    }
    std::fs::copy(filename, dst_txt)?;

    Ok(())
}

fn package_lang(src: &Path, dst: &Path, lang: &'static str) -> Result<(), Error> {
    info!("[{}] begin packaging", lang);
    // check for existence of <lang>.txt.gz/jsonl.gz
    let mut filename_txt = PathBuf::from(src);
    let mut filename_meta = PathBuf::from(src);
    filename_txt.push(format!("{}.txt.gz", lang));
    filename_meta.push(format!("{}_meta.jsonl.gz", lang));

    let mut filename_txt_multipart = PathBuf::from(src);
    filename_txt_multipart.push(format!("{}_part_1.txt.gz", lang));

    // if it exists, we operate on meta also (if only one of both exists, it's an error)
    if filename_txt.exists() {
        debug!("[{}] lang has a single txt/json file", lang);
        put_in_lang_folder(&filename_txt, dst, lang)?;
        put_in_lang_folder(&filename_meta, dst, lang)?;
        info!("[{}] done packaging", lang);
        Ok(())
    } else if filename_txt_multipart.exists() {
        debug!("[{}] lang has multiple txt/json files", lang);
        // try conversion to string
        let filename_stub = src
            .to_str()
            .ok_or_else(|| Error::Custom(format!("invalid source file: {:?}", src)))?
            .to_string();

        let filename_txt_stub = format!("{}/{}_part_*.txt.gz", filename_stub, lang);
        let filename_meta_stub = format!("{}/{}_meta_part_*.jsonl.gz", filename_stub, lang);
        // warning: does not (actually) check if for part n, both txt.gz and jsonl.gz exists.
        let text_paths = glob::glob(&filename_txt_stub)?;
        let meta_paths = glob::glob(&filename_meta_stub)?;
        for f in text_paths.chain(meta_paths) {
            put_in_lang_folder(&f?, dst, lang)?;
        }
        info!("[{}] done packaging", lang);
        Ok(())
    } else {
        warn!("[{}] no files found", lang);
        Ok(())
    }
}
pub fn package(src: &Path, dst: &Path) -> Result<(), Error> {
    let langs = LANG.clone().into_par_iter();
    let results: Vec<Error> = langs
        .filter_map(|lang| package_lang(src, dst, lang).err())
        .collect();

    if !results.is_empty() {
        for error in results {
            error!("{:?}", error);
        }
        Err(Error::Custom(
            "Errors occurred during packaging: see previous messages.".to_string(),
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{gen_checksum_file, package};

    #[test]
    fn test_hasher() {
        // gen_checksum_file(Path::new("dst_dedup_split/"));
        package(
            Path::new("dst_dedup_split_compressed"),
            Path::new("dst_dedup_split_compressed_packaged"),
        )
        .unwrap();
    }
}
