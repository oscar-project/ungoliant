//! Transform trait.
use crate::pipeline::Document;

pub trait Transform {
    /// Takes ownership of [Document] and returns it.
    fn transform_own(&self, doc: Document) -> Document;
}
