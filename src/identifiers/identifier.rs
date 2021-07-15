use crate::error::Error;
pub trait Identifier {
    fn identify(&self, sentence: &str) -> Result<Option<&'static str>, Error>;
}
