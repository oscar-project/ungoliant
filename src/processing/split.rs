/*! splitting

Offline corpus splitting.

`part_size` has to be specified in Bytes here.
!*/
use crate::{
    io::{
        reader::{reader::Reader, Corpus},
        writer::WriterTrait,
        Writer,
    },
    pipelines::oscarmeta::types::MergedPiece,
};
use log::info;
use rayon::prelude::*;
use std::path::Path;

/// Split language in chunks of provided `part_size` (bytes).
///
/// Note that metadata are splitted too but not by `part_size`, but when textual content is split.
/// However metadata are usually lighter than textual data.
///
/// It is strongly advised to use a bufsize to improve performance drastically. A value of 1000 seems to be good.
fn split_lang(
    dst: &Path,
    lang: &'static str,
    reader: Reader,
    part_size: u64,
    bufsize: Option<usize>,
) {
    info!("[{}] starting splitting ", lang);
    let mut writer = Writer::new(dst, lang, Some(part_size)).unwrap();

    let mut buf = bufsize.map(Vec::with_capacity);

    for piece in reader {
        // todo remove unwrap here
        let piece = piece.unwrap();
        // add to buffer if there's one
        // or write directly
        match &mut buf {
            Some(b) => {
                b.push(MergedPiece::from(piece.clone()));
                if b.len() == bufsize.unwrap() {
                    writer.write(b.clone()).unwrap();
                    b.clear();
                }
            }
            None => {
                writer
                    .write_single(&MergedPiece::from(piece.clone()))
                    .unwrap();
            }
        }
    }

    // write last buffer
    if let Some(b) = buf {
        if !b.is_empty() {
            writer.write(b).unwrap();
        }
    }

    // close metadata file
    // writer.close_meta().unwrap();
    info!("[{}] splitting done", lang);
}

/// Split the whole corpus, using a thread by language (max. number of threads of the machine)
pub fn split(src: &Path, dst: &Path, part_size: u64, bufsize: Option<usize>) {
    let corpus = Corpus::new(src);
    let readers_iter = corpus.readers.into_par_iter();
    readers_iter.for_each(|(lang, reader)| {
        split_lang(dst, lang, reader, part_size * 1_000_000, bufsize);
    });
}
