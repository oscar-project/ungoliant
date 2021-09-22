/*! Record location encoding

Rebuilding OSCAR needs some information about a record location both in generated corpus and in record id.

This module holds location structure as in corpus, shard, or both.
For the [Both] version, an Avro-compatible struct is also present and named [BothAvro].

!*/

use std::hash::Hasher;

use crate::io::reader::reader::PieceMeta;
use serde::{Deserialize, Serialize};

use twox_hash::XxHash64;

/**
Location types
*/
pub enum Location {
    Corpus(Corpus),
    Shard(Shard),
    Both(Both),
}

#[derive(Debug, Default)]
/** represents an entry in the corpus by its id, its (line) offset and nb_sentences, along with the starting (loc)ation of it in the file.
Also stores first sentence hash.
*/
pub struct Corpus {
    offset: usize,
    nb_sentences: usize,
    start_hash: u64, // hash of the starting line
    loc: u64,
}

impl Corpus {
    /// Set the corpus's loc.
    pub fn set_loc(&mut self, loc: u64) {
        self.loc = loc;
    }

    /// Adds the shard location, converting the [Corpus] location to a [Both] one
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
            start_hash: self.start_hash,
            shard_number,
            shard_record_number,
        }
    }

    /// Set the corpus's nb sentences.
    pub fn set_nb_sentences(&mut self, nb_sentences: usize) {
        self.nb_sentences = nb_sentences;
    }

    /// Set the corpus's start hash.
    pub fn set_start_hash(&mut self, start_hash: u64) {
        self.start_hash = start_hash;
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
/// - `start_hash`: hash of first sentence
#[derive(Debug, Clone)]
pub struct Both {
    record_id: String,
    corpus_offset_lines: usize,
    nb_sentences: usize,
    corpus_offset_bytes: u64,
    start_hash: u64,
    shard_number: u64,
    shard_record_number: usize,
}

/// Avro-safe version (with u64 as i64).
///
/// Avro's long type is equivalent to i64, so we need to cast our u64 into i64.
#[derive(Debug, Serialize, Deserialize)]
pub struct BothAvro {
    record_id: String,
    corpus_offset_lines: usize,
    nb_sentences: usize,
    corpus_offset_bytes: i64,
    start_hash: i64,
    shard_number: i64,
    shard_record_number: usize,
}

impl From<Both> for BothAvro {
    fn from(b: Both) -> BothAvro {
        BothAvro {
            record_id: b.record_id,
            corpus_offset_lines: b.corpus_offset_lines,
            nb_sentences: b.nb_sentences,
            shard_record_number: b.shard_record_number,
            corpus_offset_bytes: b.corpus_offset_bytes as i64,
            start_hash: b.start_hash as i64,
            shard_number: b.shard_number as i64,
        }
    }
}

impl From<BothAvro> for Both {
    fn from(b: BothAvro) -> Both {
        Both {
            record_id: b.record_id,
            corpus_offset_lines: b.corpus_offset_lines,
            nb_sentences: b.nb_sentences,
            shard_record_number: b.shard_record_number,
            corpus_offset_bytes: b.corpus_offset_bytes as u64,
            start_hash: b.start_hash as u64,
            shard_number: b.shard_number as u64,
        }
    }
}

impl Both {
    /// Get a reference to the both's record id.
    pub fn record_id(&self) -> &str {
        self.record_id.as_str()
    }

    /// Get a reference to the both's shard record number.
    pub fn shard_record_number(&self) -> &usize {
        &self.shard_record_number
    }

    /// Get a reference to the both's start hash.
    pub fn start_hash(&self) -> &u64 {
        &self.start_hash
    }

    /// Set the both's start hash.
    pub fn set_start_hash(&mut self, start_hash: u64) {
        self.start_hash = start_hash;
    }

    /// Get a reference to the both's nb sentences.
    pub fn nb_sentences(&self) -> &usize {
        &self.nb_sentences
    }
}

impl From<PieceMeta> for Corpus {
    fn from(piece: PieceMeta) -> Corpus {
        let mut hasher = XxHash64::default();
        hasher.write(piece.sentences.first().unwrap().as_bytes());
        Corpus {
            offset: piece.headers.offset,
            nb_sentences: piece.headers.nb_sentences,
            loc: 0,
            start_hash: hasher.finish(),
            // start_hash: u64::MAX,
        }
    }
}
