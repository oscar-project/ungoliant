/*!
# IO utilities

Textual/contextual data saving and loading.

Currently only saving is implemented but loading is planned in order to facilitate operations on already generated corpora.
!*/
mod langfiles;
mod reader;
mod writer;
pub use langfiles::LangFiles;
