use crate::error::Error;
use crate::io::reader::reader::PieceMeta;
use crate::io::reader::Corpus;
use crate::io::Writer;
use crate::processing::MergedPiece;
use log::info;
use rayon::prelude::*;
use runiq::filters::Filter;
use std::path::Path;

/// Trait for deduplication feature.
pub trait Dedup {
    fn dedup(&mut self) -> Self;
}

// impl Dedup for crate::io::reader::reader::PieceMeta {
// 	fn dedup()
// }

/// deduplicates a piece.
///
/// returns the provided offset if the piece is only composed of duplicate data.
pub fn dedup_piece(
    piece: &mut PieceMeta,
    new_offset: usize,
    filter: &mut impl Filter,
) -> Option<usize> {
    let filtered: Vec<String> = piece
        .sentences
        .iter()
        .filter(|sentence| filter.detect(&sentence.as_bytes()))
        .map(String::from)
        .collect();
    let nb_sentences = filtered.len();

    if nb_sentences == 0 {
        return None;
    }

    piece.headers.offset = new_offset;
    piece.headers.nb_sentences = nb_sentences;
    piece.sentences = filtered;

    Some(new_offset + nb_sentences + 1)
}

pub fn dedup(src: &Path, dst: &Path) -> Result<(), Error> {
    let corpus = Corpus::new(src);
    let readers_iter = corpus.readers.into_par_iter();
    readers_iter.for_each(|(lang, reader)| {
        info!("[{}] beginning deduplication", lang);
        let mut writer = Writer::new(dst, lang, None).unwrap();
        let mut filter = runiq::filters::DigestFilter::default();
        let mut offset = 0;
        for piece in reader {
            let mut piece = piece.unwrap();
            if let Some(new_offset) = dedup_piece(&mut piece, offset, &mut filter) {
                writer
                    .write_single(&MergedPiece::from(piece.clone()))
                    .unwrap();
                offset = new_offset;
            }
            // match dedup_piece(&mut piece, offset, &mut filter) {
            //     Some(new_offset) => {
            //         writer
            //             .write_single(&MergedPiece::from(piece.clone()))
            //             .unwrap();
            //         offset = new_offset;
            //     }
            //     None => (),
            // }
        }

        writer.close_meta().unwrap();
        info!("[{}] deduplication done", lang);
    });
    Ok(())
}

#[cfg(test)]
mod tests {

    use runiq::filters::Filter;

    use crate::{
        io::reader::reader::PieceMeta,
        processing::{dedup::dedup::dedup_piece, Metadata},
    };

    #[test]
    fn test_dedup_piece_single() {
        let mut filter = runiq::filters::DigestFilter::new();
        let mut piece = PieceMeta {
            sentences: ["hello", "how are you?", "goodbye!", "goodbye!"]
                .iter()
                .map(|x| x.to_string())
                .collect(),
            identification: "en",
            headers: Metadata {
                nb_sentences: 4,
                ..Default::default()
            },
        };

        let expected = PieceMeta {
            sentences: ["hello", "how are you?", "goodbye!"]
                .iter()
                .map(|x| x.to_string())
                .collect(),
            identification: "en",
            headers: Metadata {
                nb_sentences: 3,
                ..Default::default()
            },
        };

        dedup_piece(&mut piece, 0, &mut filter);
        assert_eq!(piece, expected);
    }

    #[test]
    fn test_dedup_piece_multiple() {
        let mut filter = runiq::filters::DigestFilter::new();
        let mut pieces = vec![
            PieceMeta {
                // this one shouldn't be altered
                sentences: ["hello", "how are you?", "goodbye!"]
                    .iter()
                    .map(|x| x.to_string())
                    .collect(),
                identification: "en",
                headers: Metadata {
                    offset: 0,
                    nb_sentences: 4,
                    ..Default::default()
                },
            },
            PieceMeta {
                // this one has internal and external duplicates
                sentences: [
                    "hello",
                    "hi",
                    "hi",
                    "how are you?",
                    "goodbye!",
                    "goodbye!",
                    "hi",
                    "hi",
                    "sentence that is repeated in the next piece",
                ]
                .iter()
                .map(|x| x.to_string())
                .collect(),
                identification: "en",
                headers: Metadata {
                    offset: 5,
                    nb_sentences: 9,
                    ..Default::default()
                },
            },
            PieceMeta {
                // this one only has an external duplicate
                sentences: [
                    "hello pals!",
                    "sentence that is repeated in the next piece",
                    "goodbye pals!",
                ]
                .iter()
                .map(|x| x.to_string())
                .collect(),
                identification: "en",
                headers: Metadata {
                    nb_sentences: 3,
                    offset: 15,
                    ..Default::default()
                },
            },
            // this one is only composed of duplicates and should be empty after filtering.
            PieceMeta {
                sentences: [
                    "hello",
                    "sentence that is repeated in the next piece",
                    "goodbye!",
                ]
                .iter()
                .map(|x| x.to_string())
                .collect(),
                identification: "en",
                headers: Metadata {
                    nb_sentences: 3,
                    offset: 19,
                    ..Default::default()
                },
            },
            PieceMeta {
                sentences: [
                    "completely unique",
                    "piece that comes after",
                    "completely duplicate one",
                ]
                .iter()
                .map(|x| x.to_string())
                .collect(),
                identification: "en",
                headers: Metadata {
                    nb_sentences: 3,
                    offset: 23,
                    ..Default::default()
                },
            },
        ];

        // let expected = PieceMeta {
        //     sentences: ["hello", "how are you?", "goodbye!"]
        //         .iter()
        //         .map(|x| x.to_string())
        //         .collect(),
        //     headers: Metadata {
        //         nb_sentences: 3,
        //         ..Default::default()
        //     },
        // };

        let mut next_offset = 0;
        let mut res = Vec::new();
        for piece in &mut pieces {
            if let Some(new_offset) = dedup_piece(piece, next_offset, &mut filter) {
                next_offset = new_offset;
                res.push(piece);
            }
        }

        println!("{:#?}", res);
    }
}
