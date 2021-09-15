/*! Corpus reading utilities

Code is organized in the same manner as the [crate::io::writer] mod, with {text/meta}reader and a reader that contains both for a given language.

!*/
pub mod corpus;
mod metareader;
pub mod reader;
mod textreader;

pub use corpus::Corpus;
pub use textreader::ReaderTrait;
