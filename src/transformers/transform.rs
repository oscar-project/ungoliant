//! Transform trait.

use crate::pipelines::oscardoc::types::Document;
pub trait Transform {
    /// Takes ownership of [Document] and returns it.
    fn transform_own(&self, doc: Document) -> Document;
}
