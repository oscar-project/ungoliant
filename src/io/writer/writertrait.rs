use std::path::Path;

use crate::error::Error;

use super::Writer;

pub trait WriterTrait {
    type Item;

    fn new(dst: &Path, lang: &'static str, max_file_size: Option<u64>) -> Result<Self, Error>
    where
        Self: Sized;
    fn write(&mut self, vals: Vec<Self::Item>) -> Result<(), Error>;
    fn write_single(&mut self, val: &Self::Item) -> Result<(), Error>;
    fn close_meta(&mut self) -> Result<(), Error>;
}
