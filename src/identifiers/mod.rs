/*! Language identification models

Holds an [Identifier] trait for implementing other ones.

The current identifier used is [fasttext](https://fasttext.cc)
!*/
mod fasttext;
mod identifier;
mod multilingual;

pub use self::fasttext::FastText;
pub use identifier::Identification;
pub use identifier::Identifier;
pub use multilingual::Multilingual;
pub use multilingual::StrictMultilingual;
