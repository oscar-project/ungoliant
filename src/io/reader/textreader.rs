/*! Reading facilities

Readers implement [Iterator] in order to properly iterate on sentence groups.

There are two kinds of readers:

- [LineReader] : Reads lines, no access to file position.
- [ByteReader] : Reads bytes (implements [Iterator]) on sentences groups too.

!*/
use std::{
    fs::File,
    io::{BufRead, BufReader, Lines, Read, Seek},
    path::{Path, PathBuf},
};

use crate::error::Error;

/// Enables iterating and lang retreival.
pub trait ReaderTrait: Iterator {
    fn lang(&self) -> &'static str;
}

/// Holds different kinds of Readers
#[derive(Debug)]
pub enum ReaderKind<T>
where
    T: Read,
{
    Byte(ByteReader<T>),
    Line(LineReader<T>),
}

impl<T> ReaderTrait for ReaderKind<T>
where
    T: Read,
{
    fn lang(&self) -> &'static str {
        match self {
            Self::Byte(e) => e.lang(),
            Self::Line(e) => e.lang(),
        }
    }
}

impl<T> Iterator for ReaderKind<T>
where
    T: Read,
{
    type Item = Result<Vec<String>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ReaderKind::Byte(r) => r.next(),
            ReaderKind::Line(r) => r.next(),
        }
    }
}

/// Byte-oriented reader, useful for rebuilding.
///
/// Prefer [LineReader] for more practical reading of corpus files.
#[derive(Debug)]
pub struct ByteReader<T>
where
    T: Read,
{
    path: PathBuf,
    br: BufReader<T>,
    lang: &'static str,
}

impl<T> ByteReader<T>
where
    T: Read,
{
    /// Get next line (read until `\n`)
    fn next_line(&mut self) -> Option<Result<String, Error>> {
        let mut s = String::new();
        match self.br.read_line(&mut s) {
            Ok(0) => None,
            Err(e) => Some(Err(Error::Io(e))),
            _ => Some(Ok(s)),
        }
    }

    pub fn lang(&self) -> &'static str {
        self.lang
    }
}

/// Reader that yields sequences of strings
/// that are newline separated.
#[derive(Debug)]
pub struct LineReader<T> {
    path: PathBuf,
    lines: Lines<BufReader<T>>,
    lang: &'static str,
}

pub type ByteTextReader = ByteReader<File>;
pub type TextReader = LineReader<File>;

impl ByteTextReader {
    pub fn new(src: &Path, lang: &'static str) -> Result<Self, Error> {
        let filename = format!("{}.txt", lang);
        let mut src = src.to_path_buf();
        src.push(filename);
        let texthandler = File::open(&src)?;
        let br = BufReader::new(texthandler);
        Ok(Self {
            path: src,
            br,
            lang,
        })
    }

    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Returns the position in the stream. See [std::io::Seek::stream_position] for more details.
    pub fn pos(&mut self) -> Result<u64, Error> {
        Ok(self.br.stream_position()?)
    }
}

impl<T> LineReader<T> {
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    fn lang(&self) -> &'static str {
        self.lang
    }
}
impl TextReader {
    pub fn new(src: &Path, lang: &'static str) -> Result<Self, Error> {
        Ok(ByteTextReader::new(src, lang)?.into())
    }
}

impl From<ByteTextReader> for TextReader {
    fn from(tr: ByteTextReader) -> TextReader {
        TextReader {
            path: tr.path().to_owned(),
            lines: tr.br.lines(),
            lang: tr.lang,
        }
    }
}

impl<T> Iterator for LineReader<T>
where
    T: Read,
{
    type Item = Result<Vec<String>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut ret = Vec::new();
        while let Some(Ok(sen)) = self.lines.next() {
            if sen.is_empty() {
                return Some(ret.into_iter().collect());
            }
            ret.push(Ok(sen));
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret.into_iter().collect())
        }
    }
}

impl<T> Iterator for ByteReader<T>
where
    T: Read,
{
    type Item = Result<Vec<String>, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut ret = Vec::new();
        while let Some(Ok(sen)) = self.next_line() {
            if sen.is_empty() {
                return Some(ret.into_iter().collect());
            }
            ret.push(Ok(sen));
        }

        if ret.is_empty() {
            None
        } else {
            Some(ret.into_iter().collect())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufReader;

    #[test]
    fn test_iter() {
        let sentences = std::io::Cursor::new(
            "aaa
bbb
ccc

record 2
this is record 2
end of record 2

bye!
record 3",
        );

        let expected = vec![
            vec!["aaa", "bbb", "ccc"],
            vec!["record 2", "this is record 2", "end of record 2"],
            vec!["bye!", "record 3"],
        ];

        let br = BufReader::new(sentences);
        let tr = LineReader {
            path: PathBuf::new(), //empty, for testing
            lines: br.lines(),
            lang: "en",
        };
        for (res, exp) in tr.zip(expected.iter()) {
            let res = res.unwrap();
            assert_eq!(&res, exp);
        }
    }

    #[test]
    fn test_iter_single_record() {
        let sentences = std::io::Cursor::new(
            "aaa
bbb
ccc
record 1
this is record 1
end of record 1
bye!
record 1",
        );

        let expected = vec![vec![
            "aaa",
            "bbb",
            "ccc",
            "record 1",
            "this is record 1",
            "end of record 1",
            "bye!",
            "record 1",
        ]];

        let br = BufReader::new(sentences);
        let tr = LineReader {
            path: PathBuf::new(),
            lines: br.lines(),
            lang: "en",
        };
        for (res, exp) in tr.zip(expected.iter()) {
            let res = res.unwrap();
            assert_eq!(&res, exp);
        }
    }
}
