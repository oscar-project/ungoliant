/*! Reader for a specific language.
!*/
use std::path::Path;

use crate::{
    error::Error,
    processing::{MergedPiece, Metadata},
};

use super::{metareader::MetaReader, textreader::TextReader};

/// Analoguous to [MergedPiece] but containing [Metadata].
///
/// Is convertible to [MergedPiece].  
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PieceMeta {
    pub sentences: Vec<String>,
    pub headers: Metadata,
    pub identification: &'static str,
}

impl From<PieceMeta> for MergedPiece {
    fn from(pm: PieceMeta) -> MergedPiece {
        MergedPiece {
            headers: pm
                .headers
                .headers
                .into_iter()
                .map(|(k, v)| (k, v.as_bytes().to_vec()))
                .collect(),
            sentences: pm.sentences.join("\n"),
            nb_sentences: pm.headers.nb_sentences,
            identification: pm.identification,
        }
    }
}
#[derive(Debug)]
pub struct Reader {
    textreader: TextReader,
    metareader: MetaReader,
    lang: &'static str,
}

impl Reader {
    /// Create a new reader.
    ///
    /// Propagates errors from inner [TextReader] and [MetaReader].
    pub fn new(dst: &Path, lang: &'static str) -> Result<Self, Error> {
        let textreader = TextReader::new(dst, lang)?;
        let metareader = MetaReader::new(dst, lang)?;

        Ok(Self {
            textreader,
            metareader,
            lang,
        })
    }
}

impl Iterator for Reader {
    type Item = Result<PieceMeta, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.textreader.next(), self.metareader.next()) {
            // default case when everything is good: we yield.
            (Some(Ok(sentences)), Some(Ok(metadata))) => Some(Ok(PieceMeta {
                sentences,
                headers: metadata,
                identification: self.textreader.lang,
            })),
            // If text or meta readers return some error, propagate it
            (_, Some(Err(e))) | (Some(Err(e)), _) => Some(Err(e)),

            // If only one iterator is finished, return a custom error
            (Some(_), None) | (None, Some(_)) => Some(Err(Error::Custom(
                "sync problem between metadata and sentences".to_string(),
            ))),

            // End of the iterator
            (None, None) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    //TODO
}
