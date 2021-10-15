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
    /// generates readers from the list of languages in [crate::lang::LANG]
    ///
    /// Erorrs are *not* returned but rather printed out if some language files are not found.
    fn get_file_list(src: &Path) -> HashMap<&'static str, Reader> {
        let (readers, errors): (HashMap<_, _>, HashMap<_, _>) = LANG
            .iter()
            .map(|lang| (*lang, Reader::new(src, lang)))
            .partition(|(_, v)| v.is_ok());

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

    // Create a new Corpus reader.
    pub fn new(src: &Path) -> Self {
        Self {
            readers: Self::get_file_list(src),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::Corpus;

    #[test]
    fn test_new() {
        let _ = Corpus::new(Path::new("dst/"));
    }
}
