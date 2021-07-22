/*! Language identification models

Holds an [Identifier] trait for implementing other ones.

The current identifier used is [fasttext](https://fasttext.cc)
!*/
mod fasttext;
mod identifier;

pub use self::fasttext::FastText;
pub use identifier::Identifier;
