/*!
# Rotating file based writing
This module deals with writing text and metadata to files, following a size limit for text files.

The user-facing object is [LangFiles], which holds a Mutex-guarded [Writer] for each language.

Each [Writer] is composed of a [TextWriter]/[MetaWriter] couple, with [TextWriter] creating new files when a provided limit is reached.
[TextWriter] has a flag that is set to `true` when a new file is opened, is checked manually by [Writer] to properly notify [MetaWriter] to create a new file too.

This leads the [TextWriter]/[MetaWriter] couple to be cumbersome to use outside of [Writer].
!*/
mod metawriter;
mod textwriter;
pub mod writer;
mod writer_doc;
use metawriter::MetaWriter;
use textwriter::TextWriter;
pub use writer::Writer;
pub use writer_doc::WriterDoc;
