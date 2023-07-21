/*! Packaging (prepping for distribution) utilities

Packaging is in two steps:
- First, we create a folder for each present language, and we move language files (text and metadata) into them
- Then, we compute a sha384sum for each file, and write them into language independent files, _usually_ compatible with `sha384sum -c` implementations.
!*/
use log::warn;
use rayon::prelude::*;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use log::error;
use log::{debug, info};
use sha2::Digest;
use sha2::Sha384;

use crate::error::Error;
// use crate::lang::LANG;

/// Create checksum file for given lang.
/// Note that the `src` path supplied is the source path of the **whole** corpus, not the language dependent one.
pub fn gen_checksum_file(src: &Path, lang: &'static str) -> Result<(), Error> {
    let mut src_lang = PathBuf::from(src);
    src_lang.push(lang);

    debug!("gen checksum on folder {:?}", src_lang);
    let mut hasher = Sha384::new();
    let files: Vec<_> = std::fs::read_dir(&src_lang)?.collect();

    debug!("files to hash: {:#?}", files);
    let mut hashes: Vec<String> = Vec::with_capacity(files.len());
    let mut filenames: Vec<String> = Vec::with_capacity(files.len());

    for f in files {
        let f = f?;

        // push hash and filename
        info!("[{}] hashing {:?}", lang, f.file_name());
        hashes.push(get_hash(&f.path(), &mut hasher)?);
        filenames.push(f.file_name().to_string_lossy().into_owned());
    }

    // forge filepath of checksum file
    let checksum_filepath: PathBuf = [
        src_lang,
        Path::new(&format!("{}_sha384.txt", lang)).to_path_buf(),
    ]
    .iter()
    .collect();

    debug!("writing hashes to: {:?}", checksum_filepath);
    // open it
    let mut checksum_file = File::create(checksum_filepath)?;

    // write filenames and hashes in sha384sum -c compatible format.
    for (filename, hash) in filenames.iter().zip(hashes) {
        writeln!(&mut checksum_file, "{} {}", hash, filename)?;
    }

    Ok(())
}

/// compute the hash of the file pointed by the filepath by using [io::copy] between a file handler and the hasher.
/// As such, it shouldn't make the program go OOM with big files, but it has not been tested.
/// Can return an error if there has been problems regarding IO.
#[inline]
fn get_hash(filepath: &Path, hasher: &mut Sha384) -> Result<String, Error> {
    let mut f = File::open(filepath)?;
    io::copy(&mut f, hasher)?;
    let result = format!("{:x}", hasher.finalize_reset());
    Ok(result)
}

/// moves `<lang>*.{txt, jsonl}.gz` to `dst/<lang>/<lang>*.{txt, jsonl}.gz`, creating the folder if it did not exist.
fn put_in_lang_folder(
    filename: &Path,
    dst: &Path,
    lang: &'static str,
    move_files: bool,
) -> Result<(), Error> {
    let mut dst = PathBuf::from(dst);
    dst.push(lang);
    let mut dst_txt = dst.clone();
    dst_txt.push(filename.file_name().unwrap());

    //return error only if it's not an AlreadyExists error
    if let Err(e) = std::fs::create_dir(dst) {
        if e.kind() != std::io::ErrorKind::AlreadyExists {
            return Err(Error::Io(e));
        }
    }

    // move or copy depending on flag
    if move_files {
        std::fs::rename(filename, dst_txt)?;
    } else {
        std::fs::copy(filename, dst_txt)?;
    }

    Ok(())
}

/// Iteratively moves each file corresponding to `lang` into a proper folder named by the language id,
/// then computes sha384sum for each file.
///
/// - `src` is the corpus location, containing language files in compressed format.
/// - `dst` is the packaged corpus location.
///
/// Packaging does not (yet) provide in-place operation.
fn package_lang(
    src: &Path,
    dst: Option<&Path>,
    lang: &'static str,
    move_files: bool,
) -> Result<(), Error> {
    if !move_files && dst.is_none() {
        return Err(Error::Custom("No destination path specified!".to_string()));
    }

    // set destination same as source if move flag
    let dst = match dst {
        Some(d) => d,
        None => src,
    };

    info!("[{}] begin packaging", lang);
    // forge filenames for single-part
    let mut filename_txt = PathBuf::from(src);
    let mut filename_meta = PathBuf::from(src);
    filename_txt.push(format!("{}.txt.gz", lang));
    filename_meta.push(format!("{}_meta.jsonl.gz", lang));

    // forge filenames for multi-part
    // we don't forge for meta because we assume that if txt exists, meta too
    let mut filename_txt_multipart = PathBuf::from(src);
    filename_txt_multipart.push(format!("{}_part_1.txt.gz", lang));

    // if it exists, we operate on meta also (if only one of both exists, it's an error)
    if filename_txt.exists() {
        debug!("[{}] lang has a single txt/json file", lang);
        put_in_lang_folder(&filename_txt, dst, lang, move_files)?;
        put_in_lang_folder(&filename_meta, dst, lang, move_files)?;
        gen_checksum_file(dst, lang)?;
        info!("[{}] done packaging", lang);
        Ok(())

    // try for multi-part
    } else if filename_txt_multipart.exists() {
        debug!("[{}] lang has multiple txt/json files", lang);
        // try conversion to string
        let filename_stub = src
            .to_str()
            .ok_or_else(|| Error::Custom(format!("invalid source file: {:?}", src)))?
            .to_string();

        // forge paths for globbing
        let filename_txt_stub = format!("{}/{}_part_*.txt.gz", filename_stub, lang);
        let filename_meta_stub = format!("{}/{}_meta_part_*.jsonl.gz", filename_stub, lang);

        // warning: does not (actually) check if for part n, both txt.gz and jsonl.gz exists.
        let text_paths = glob::glob(&filename_txt_stub)?;
        let meta_paths = glob::glob(&filename_meta_stub)?;

        //chain both text and meta files and move them all
        for f in text_paths.chain(meta_paths) {
            put_in_lang_folder(&f?, dst, lang, move_files)?;
        }

        // generating checksum file
        info!("[{}] generating checksums", lang);
        gen_checksum_file(dst, lang)?;
        info!("[{}] done packaging", lang);

        Ok(())
    } else {
        // It is not a hard error, since it is possible that some languages are
        // not to be found.
        warn!("[{}] no files found", lang);
        Ok(())
    }
}

/// concurrently package all the languages present in `src` (split and compressed) to `dst`
/// in separate language folders along with a `sha384sum -c`-able file.
pub fn package(src: &Path, dst: Option<&Path>, move_files: bool) -> Result<(), Error> {
    let langs = LANG.clone().into_par_iter();
    let results: Vec<Error> = langs
        .filter_map(|lang| package_lang(src, dst, lang, move_files).err())
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

    //TODO
}
