//! Rotating file writer for metadata.
use crate::error;
use log::{debug, warn};
use oxilangtag::LanguageTag;
use std::fs::OpenOptions;
use std::path::Path;
use std::{fs::File, io::Write, path::PathBuf};

/// Rotating file writer.
///
/// Implements [std::io::Write]
///
/// *Note:* Contrary to TextWriter, [MetaWriter] has no limit and new file creation has to be triggered manually by invoking [MetaWriter::create_next_file].
pub struct MetaWriter {
    lang: LanguageTag<String>,
    dst: PathBuf,
    pub file: Option<File>,
    nb_files: u64,
}

impl MetaWriter {
    /// Create a new [MetaWriter].
    /// Note that nothing is created/written unless a write is performed.
    /// size_limit is in bytes.
    pub fn new(dst: &Path, lang: LanguageTag<String>) -> Self {
        Self {
            lang,
            dst: dst.to_path_buf(),
            file: None,
            nb_files: 0,
        }
    }

    /// attempt to close current file while ending json.
    pub fn close_file(&mut self) -> Result<(), error::Error> {
        if self.file.is_some() {
            self.file = None;
        } else {
            warn!("{}: trying to close an unopened MetaWriter.", self.lang);
        }
        Ok(())
    }

    /// Rotate file.
    ///
    /// The first file is named `lang_meta.json`, and is renamed `lang_meta_part_1.json` if there's > 1 number of files.
    pub fn create_next_file(&mut self) -> std::io::Result<()> {
        let filename = if self.nb_files == 0 {
            format!("{}_meta.jsonl", self.lang)
        } else {
            format!("{}_meta_part_{}.jsonl", self.lang, self.nb_files + 1)
        };

        let mut path = self.dst.clone();
        path.push(filename);

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);

        let file = options.open(path)?;

        // if nb_files == 1
        if self.nb_files == 1 {
            let mut from = self.dst.clone();
            from.push(format!("{}_meta.jsonl", self.lang));
            let mut to = self.dst.clone();
            to.push(format!("{}_meta_part_1.jsonl", self.lang));

            debug!("renaming {:?} to {:?}", from, to);
            std::fs::rename(from, to)?;
        }

        self.file = Some(file);

        self.nb_files += 1;
        Ok(())
    }
}

impl Write for MetaWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // if there's no file open, create one
        if self.file.is_none() {
            self.create_next_file()?;
        }

        if let Some(file) = &mut self.file {
            let bytes_written = file.write(buf)?;
            Ok(bytes_written)
        } else {
            Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!(
                    "Could not write to file {} for lang {}",
                    self.nb_files, self.lang
                ),
            ))
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match &mut self.file {
            Some(file) => file.flush(),
            None => Ok(()),
        }
    }
}
