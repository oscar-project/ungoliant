use std::collections::HashMap;
use std::convert::TryFrom;
use std::io::Write;
use std::path::Path;

use ungoliant::pipeline::Metadata;
use warc::header::WarcHeader;

use crate::pipeline::oscar_metadata::document::MergedPiece;
use crate::{
    error,
    writing::{MetaWriter, TextWriter},
};

type WarcHeaders = HashMap<WarcHeader, Vec<u8>>;
pub struct Writer {
    handle_text: TextWriter,
    handle_meta: MetaWriter,
    lang: &'static str,
    offset: usize,
}

impl Writer {
    pub fn new(dst: &Path, lang: &'static str, size_limit: u64) -> Result<Self, error::Error> {
        Ok(Self {
            handle_text: TextWriter::new(dst, lang, size_limit),
            handle_meta: MetaWriter::new(dst, lang),
            lang,
            offset: 0,
        })
    }

    // writes
    pub fn write(&mut self, pieces: &[MergedPiece]) -> Result<(), error::Error> {
        for piece in pieces {
            //ensure that the piece has the correct language identification
            if piece.identification() != self.lang {
                return Err(error::Error::Custom(format!(
                    "Wrong language. Tried to add a {} piece into a {} file.",
                    piece.identification(),
                    self.lang
                )));
            }

            self.handle_text.write_all(piece.sentences.as_bytes())?;
            // trigger new file creation for metadata if applicable
            // reset offest
            if self.handle_text.first_write_on_document {
                // ignore if <= 1 since it's the first file
                if self.handle_text.nb_files > 1 {
                    self.handle_meta.create_next_file()?;
                    self.offset = 0;
                }
                self.handle_text.first_write_on_document = false;
            }

            let mut metadata = Metadata::try_from(piece.headers.clone())?;

            // update defaulted values in metadata
            metadata.nb_sentences = piece.nb_sentences;
            metadata.offset = self.offset;

            // update lang offset
            self.offset += metadata.nb_sentences + 1;

            let mut metadata_str = serde_json::to_string_pretty(&metadata).unwrap(); //todo add from for error
            metadata_str.push(',');

            self.handle_meta.write_all(metadata_str.as_bytes())?;
        }
        Ok(())
    }

    pub fn close_meta(&mut self) -> Result<(), error::Error> {
        self.handle_meta.close_file()
    }
}
#[cfg(test)]
mod tests {

    use std::{fs::File, io::Read};

    use super::*;

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
    fn test_init() {
        let dst = Path::new("dst_test_init_writer");
        std::fs::create_dir(dst).unwrap();
        let wr = Writer::new(dst, "en", 1_000_000);
    }

    #[test]
    fn write() {
        let dst = Path::new("dst_test_write");
        std::fs::create_dir(dst).unwrap();
        let mut wr = Writer::new(dst, "fr", 10).unwrap();

        let headers: WarcHeaders =
            vec![(WarcHeader::Filename, Vec::from("filenametest".as_bytes()))]
                .into_iter()
                .collect();
        let merged_pieces = vec![MergedPiece {
            sentences: "Bonjour, c'est moi!
Comment allez-vous?
Bien, et vous?
Ecoutez ça va plutôt bien."
                .to_string(),
            nb_sentences: 4,
            identification: "fr",
            headers,
        }];

        wr.write(&merged_pieces).unwrap();
        wr.close_meta().unwrap();

        // check if content is the same
        let mut sentences = String::new();
        let mut f = File::open("dst_test_write/fr.txt").unwrap();
        f.read_to_string(&mut sentences).unwrap();

        //to account for \n\n
        let mut from_merged_pieces = merged_pieces[0].sentences.clone();
        from_merged_pieces.push_str("\n\n");

        assert_eq!(sentences, from_merged_pieces);

        // succintly check if metadata are the same
        let mut f = File::open("dst_test_write/fr_meta.json").unwrap();
        let metadata: Vec<Metadata> = serde_json::from_reader(f).unwrap();
        assert_eq!(metadata[0].nb_sentences, merged_pieces[0].nb_sentences);
    }

    #[test]
    fn write_multiple() {
        let dst = Path::new("dst_test_write_multiple");
        std::fs::create_dir(dst).unwrap();
        let mut wr = Writer::new(dst, "fr", 10_000).unwrap();

        let mut merged_pieces = Vec::new();
        for i in 1..100 {
            let headers: WarcHeaders = vec![(
                WarcHeader::Filename,
                Vec::from(format!("filenametest{}", i).as_bytes()),
            )]
            .into_iter()
            .collect();

            let sentences = vec!["lorem ipsum".to_string(); i].join("\n");
            let nb_sentences = i;
            let identification = "fr";

            merged_pieces.push(MergedPiece {
                sentences,
                headers,
                nb_sentences,
                identification,
            });
        }

        wr.write(&merged_pieces).unwrap();
        wr.close_meta().unwrap();

        // check if content is the same
        let mut sentences = String::new();
        let mut f = File::open("dst_test_write/fr.txt").unwrap();
        f.read_to_string(&mut sentences).unwrap();
        assert_eq!(sentences, merged_pieces[0].sentences);

        // succintly check if metadata are the same
        let mut f = File::open("dst_test_write/fr_meta.json").unwrap();
        let metadata: Vec<Metadata> = serde_json::from_reader(f).unwrap();
        assert_eq!(metadata[0].nb_sentences, merged_pieces[0].nb_sentences);
    }
}
