/*! Identifier trait

All identifiers should implement [Identifier] to be useable in processing and pipelines.
!*/
use crate::error::Error;
pub trait Identifier {
    /// returns a language identification token (from [crate::lang::LANG]).
    fn identify(&self, sentence: &str) -> Result<Option<&'static str>, Error>;
}
