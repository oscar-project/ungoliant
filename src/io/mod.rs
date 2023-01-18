/*!
# IO utilities

Textual/contextual data saving and loading.

Currently only saving is implemented but loading is planned in order to facilitate operations on already generated corpora.
!*/
mod langfiles;
pub mod reader;
pub mod writer;
// pub use langfiles::LangFiles;
pub use langfiles::LangFilesDoc;
// pub use writer::Writer;
