/*! Thread-safe language-separated text/metadata writer.

Each language (provided by [crate::lang::LANG]) is given a [self::Writer] wrapped into an [Arc<Mutex<Writer>>].

## Warning

!*/
use std::{
    collections::HashMap,
    fs::File,
    io::Write,
    path::Path,
    str::FromStr,
    sync::{Arc, Mutex},
};

use crate::io::writer::Writer;
use crate::lang::LANG;
use crate::{error, lang::Lang};

use super::writer::{DocWriterAvro, WriterDoc, WriterTrait};
/// Holds references to [Writer].
pub struct LangFiles {
    writers: HashMap<&'static str, Arc<Mutex<Writer>>>,
}

pub struct LangFilesDoc {
    writers: HashMap<Lang, Arc<Mutex<WriterDoc>>>,
}

pub struct LangFilesAvro<'a> {
    writers: HashMap<Lang, Arc<Mutex<DocWriterAvro<'a, File>>>>,
}

impl<'a> LangFilesAvro<'a> {
    pub fn new(dst: &Path) -> Result<Self, error::Error> {
        let mut writers = HashMap::with_capacity(LANG.len());
        let mut w;
        for lang in LANG.iter() {
            let mut dst = dst.to_path_buf();
            dst.push(lang);
            dst.set_extension("avro");
            w = DocWriterAvro::from_file(&dst)?;
            let lang = Lang::from_str(lang)?;
            writers.insert(lang, Arc::new(Mutex::new(w)));
        }

        Ok(Self { writers })
    }

    pub fn writers(&'a self) -> &HashMap<Lang, Arc<Mutex<DocWriterAvro<File>>>> {
        &self.writers
    }
}
impl LangFiles {
    /// Create a new LangFiles. `part_size_bytes` sets an indication of the maximum size
    /// by part.
    /// Note that if it is set too low and a unique record can't be stored in an unique part
    /// then a part will still be created, being larger than the `part_size_bytes`. This is expected behaviour.
    ///
    /// Also keep in mind that [Self::close_meta] has to be called once every write is done.
    ///
    // [Self::close_meta] could be integrated in an `impl Drop`
    pub fn new(dst: &Path, part_size_bytes: Option<u64>) -> Result<Self, error::Error> {
        let mut writers = HashMap::with_capacity(LANG.len());
        let mut w;
        for lang in LANG.iter() {
            w = Writer::new(dst, lang, part_size_bytes)?;
            writers.insert(*lang, Arc::new(Mutex::new(w)));
        }

        Ok(Self { writers })
    }

    /// Get a non-mutable reference to the writers.
    pub fn writers(&self) -> &HashMap<&'static str, Arc<Mutex<Writer>>> {
        &self.writers
    }

    /// Fix open metadata files by removing trailing comma and closing the array.
    pub fn close_meta(&self) -> Result<(), error::Error> {
        for writer in self.writers.values() {
            let mut writer_lock = writer.lock().unwrap();
            writer_lock.close_meta()?;
        }
        Ok(())
    }
}

impl LangFilesDoc {
    /// Create a new LangFiles. `part_size_bytes` sets an indication of the maximum size
    /// by part.
    /// Note that if it is set too low and a unique record can't be stored in an unique part
    /// then a part will still be created, being larger than the `part_size_bytes`. This is expected behaviour.
    ///
    /// Also keep in mind that [Self::close_meta] has to be called once every write is done.
    ///
    // [Self::close_meta] could be integrated in an `impl Drop`
    pub fn new(dst: &Path, part_size_bytes: Option<u64>) -> Result<Self, error::Error> {
        let mut writers = HashMap::with_capacity(LANG.len());
        let mut w;
        for lang in LANG.iter() {
            w = WriterDoc::new(dst, lang, part_size_bytes)?;
            let lang = Lang::from_str(lang)?;
            writers.insert(lang, Arc::new(Mutex::new(w)));
        }

        Ok(Self { writers })
    }

    /// Get a non-mutable reference to the writers.
    pub fn writers(&self) -> &HashMap<Lang, Arc<Mutex<WriterDoc>>> {
        &self.writers
    }

    /// Fix open metadata files by removing trailing comma and closing the array.
    pub fn close_meta(&self) -> Result<(), error::Error> {
        for writer in self.writers.values() {
            let mut writer_lock = writer.lock().unwrap();
            writer_lock.close_meta()?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::{fs::File, path::PathBuf};

    use crate::{
        identifiers::Identification,
        pipelines::oscardoc::types::{Document, Metadata},
        pipelines::oscarmeta::types::MergedPiece,
    };
    use warc::{BufferedBody, Record, WarcHeader};

    use super::*;
    use tempfile::tempdir;

    type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;

    fn create_merged_piece(
        sentences: String,
        identification: &'static str,
        headers: WarcHeaders,
    ) -> MergedPiece {
        let nb_sentences = sentences.split("\n").count();
        MergedPiece {
            sentences,
            identification,
            headers,
            nb_sentences,
        }
    }
    #[test]
    fn init() {
        let dst = Path::new("dst_langfiles_init");
        std::fs::create_dir(dst).unwrap();
        let _ = LangFiles::new(dst, Some(10));
        std::fs::remove_dir_all(dst).unwrap();
    }

    #[test]
    fn write_one() {
        let dst = Path::new("dst_langfiles_write_one");
        std::fs::create_dir(dst).unwrap();
        let langfiles = LangFiles::new(dst, Some(10)).unwrap();

        let sentences = "essai d'écriture
de trois lignes
hehe :)"
            .to_string();
        let headers = vec![(WarcHeader::ContentType, Vec::from("blogpost".as_bytes()))]
            .into_iter()
            .collect();
        let mp = vec![create_merged_piece(sentences, "fr", headers)];
        // lock mutex and acquire writer
        let fr_writer = langfiles.writers().get("fr").unwrap().clone();
        let mut fr_writer_locked = fr_writer.lock().unwrap();

        fr_writer_locked.write(mp).unwrap();
        std::fs::remove_dir_all(dst).unwrap();
    }

    #[test]
    fn init_doc() {
        let dst = tempdir().unwrap();
        LangFilesDoc::new(dst.path(), None).unwrap();
    }

    #[test]
    fn write_one_doc() {
        let dst = tempdir().unwrap();
        let lf = LangFilesDoc::new(dst.path(), None).unwrap();

        let content = "Hello!".to_string();

        let record = Record::default();
        let record: Record<BufferedBody> = record.add_body(content);

        let record_id = Identification::new(Lang::En, 1.0);
        let sentences_id = vec![Some(record_id.clone())];

        let metadata = Metadata::new(&record_id, &sentences_id);
        let (headers, content) = record.into_raw_parts();

        let docs = vec![Document::new(
            String::from_utf8_lossy(&content).to_string(),
            headers.headers,
            metadata,
        )];

        let w = lf
            .writers
            .get(docs[0].identification().label())
            .unwrap()
            .clone();

        if let Ok(mut w) = w.try_lock() {
            w.write(docs.to_vec()).unwrap();
        }

        let mut read_path = PathBuf::from(dst.path());
        read_path.push("en_meta.jsonl");

        let b = File::open(read_path).unwrap();
        let doc_from_file: Document = serde_json::from_reader(b).unwrap();

        assert_eq!(doc_from_file, docs[0]);
    }
}
