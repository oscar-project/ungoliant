use std::{collections::HashMap, path::Path};

use log::{error, warn};

use crate::error::Error;
use crate::lang::LANG;

use super::reader::Reader;
pub struct Corpus {
    pub readers: HashMap<&'static str, Reader>,
}

impl Corpus {
    fn get_file_list(src: &Path) -> HashMap<&'static str, Reader> {
        // let (results, failures): (HashMap<_, _>, HashMap<_, _>) = LANG

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
        let c = Corpus::new(Path::new("dst/"));
    }
}
