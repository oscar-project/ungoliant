/*! Language identification models

Holds an [Identifier] trait for implementing other ones.

The current identifier used is [fasttext](https://fasttext.cc) !*/
pub(crate) mod identification;
pub(crate) mod model;
mod multilingual;
mod tag_convert;

pub use multilingual::Multilingual;
pub use multilingual::StrictMultilingual;
