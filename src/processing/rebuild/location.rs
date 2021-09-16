/*! Patching for <1.2 OSCAR Schema !*/

use crate::io::reader::reader::PieceMeta;
use serde::{Deserialize, Serialize};

pub enum Location {
    Corpus(Corpus),
    Shard(Shard),
    Both(Both),
}

#[derive(Debug)]
/// represents an entry in the corpus by its id, its (line) offset and nb_sentences, along with the starting location of it in the file.
pub struct Corpus {
    offset: usize,
    nb_sentences: usize,
    loc: u64,
}

impl Corpus {
    /// Set the corpus's loc.
    pub fn set_loc(&mut self, loc: u64) {
        self.loc = loc;
    }

    pub fn add_shard_loc(
        &self,
        record_id: &str,
        shard_number: u64,
        shard_record_number: usize,
    ) -> Both {
        Both {
            record_id: record_id.to_owned(),
            corpus_offset_lines: self.offset,
            nb_sentences: self.nb_sentences,
            corpus_offset_bytes: self.loc,
            shard_number,
            shard_record_number,
        }
    }
}

#[derive(Debug)]
/// represents an entry in the shard by its id, its (first) and (last) lines (relative to the record), alongh with the starting location of it in the file.
pub struct Shard {
    first: usize,
    last: usize,
    loc: usize,
}

/// represents a record in its location both in corpus and shards.
///
/// - `corpus_offset_lines`: offset (in lines) to the beginning of the record text (0=start of the file).
/// - `nb_sentences`: number of sentences present in the record's text. Last sentence line location is `offset+nb_sentences`.
/// - `corpus_offset_bytes`: offset (in bytes) to the beginning of the record text (0=start of the file). Useful for seeking.
/// - `shard_number`: shard number where the record is located.
/// - `shard_record_number`: offset (in records) to the record.
#[derive(Debug, Serialize, Deserialize)]
pub struct Both {
    record_id: String,
    corpus_offset_lines: usize,
    nb_sentences: usize,
    corpus_offset_bytes: u64,

    shard_number: u64,
    shard_record_number: usize,
}

impl From<PieceMeta> for Corpus {
    fn from(piece: PieceMeta) -> Corpus {
        Corpus {
            offset: piece.headers.offset,
            nb_sentences: piece.headers.nb_sentences,
            loc: 0,
        }
    }
}
