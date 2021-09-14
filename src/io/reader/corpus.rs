/*! Corpus reader

Holds a [Reader] for each lang declared in [crate::lang::LANG].
!*/
use std::{collections::HashMap, path::Path};

use log::{error, warn};

use crate::error::Error;
use crate::lang::LANG;

use super::reader::Reader;
pub struct Corpus {
    pub readers: HashMap<&'static str, Reader>,
}

impl Corpus {
    /// get (line) readers
    fn readers(
        src: &Path,
    ) -> (
        HashMap<&'static str, Result<Reader, Error>>,
        HashMap<&'static str, Result<Reader, Error>>,
    ) {
        LANG.iter()
            .map(|lang| (*lang, Reader::new(src, lang)))
            .partition(|(_, v)| v.is_ok())
    }

    /// get byte readers. See [Reader] for more info.
    fn readers_byte(
        src: &Path,
    ) -> (
        HashMap<&'static str, Result<Reader, Error>>,
        HashMap<&'static str, Result<Reader, Error>>,
    ) {
        LANG.iter()
            .map(|lang| (*lang, Reader::new_bytes(src, lang)))
            .partition(|(_, v)| v.is_ok())
    }

    fn filter_errors(
        readers: HashMap<&'static str, Result<Reader, Error>>,
        errors: HashMap<&'static str, Result<Reader, Error>>,
    ) -> HashMap<&'static str, Reader> {
        let readers: HashMap<&'static str, Reader> =
            readers.into_iter().map(|(k, v)| (k, v.unwrap())).collect();

        let errors: HashMap<&'static str, Error> = errors
            .into_iter()
            .map(|(k, v)| (k, v.unwrap_err()))
            .collect();

        for (lang, e) in errors {
            match e {
                // care, as it does not check whether contained io error is NotFound
                Error::Io(_) => warn!("[{:#?}] no text/meta file.", lang),
                e => error!("[{:#?}] something wrong happened: {:#?}", lang, e),
            }
        }

        readers
    }
    /// generates readers from the list of languages in [crate::lang::LANG]
    ///
    /// Erorrs are *not* returned but rather printed out if some language files are not found.
    fn get_file_list(src: &Path) -> HashMap<&'static str, Reader> {
        let (readers, errors) = Self::readers(src);
        Self::filter_errors(readers, errors)
    }

    /// generates byte readers from the list of languages in [crate::lang::LANG]
    ///
    /// Erorrs are *not* returned but rather printed out if some language files are not found.
    fn get_file_list_bytes(src: &Path) -> HashMap<&'static str, Reader> {
        let (readers, errors) = Self::readers_byte(src);
        Self::filter_errors(readers, errors)
    }

    // Create a new Corpus reader.
    pub fn new(src: &Path) -> Self {
        Self {
            readers: Self::get_file_list(src),
        }
    }

    /// Create a new Corpus reader that has the ability to read byte by byte.
    ///
    /// See [super::textreader::ByteReader]
    pub fn new_bytes(src: &Path) -> Self {
        Self {
            readers: Self::get_file_list_bytes(src),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::Corpus;

    #[test]
    fn test_new() {
        let c = Corpus::new(Path::new("dst/"));
    }
}
