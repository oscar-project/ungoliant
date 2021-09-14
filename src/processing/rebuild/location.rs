/*! Patching for <1.2 OSCAR Schema !*/

use crate::io::reader::reader::PieceMeta;

pub enum Location {
    Corpus(Corpus),
    Shard(Shard),
    Both(Both),
}

/// represents an entry in the corpus by its id, its (line) offset and nb_sentences, along with the starting location of it in the file.
pub struct Corpus {
    offset: usize,
    nb_sentences: usize,
    loc: usize,
}

impl Corpus {
    /// Set the corpus's loc.
    pub fn set_loc(&mut self, loc: usize) {
        self.loc = loc;
    }
}

/// represents an entry in the shard by its id, its (first) and (last) lines (relative to the record), alongh with the starting location of it in the file.
pub struct Shard {
    first: usize,
    last: usize,
    loc: usize,
}

pub struct Both {
    offset: usize,
    nb_sentences: usize,
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
