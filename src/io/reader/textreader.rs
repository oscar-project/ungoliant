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
    fn pos(&mut self) -> Option<Result<u64, Error>>;
}

/// Holds different kinds of Readers
#[derive(Debug)]
pub enum ReaderKind<T>
where
    T: Read + Seek,
{
    Byte(ByteReader<T>),
    Line(LineReader<T>),
}

impl<T> ReaderTrait for ReaderKind<T>
where
    T: Read + Seek,
{
    fn lang(&self) -> &'static str {
        match self {
            Self::Byte(e) => e.lang(),
            Self::Line(e) => e.lang(),
        }
    }

    fn pos(&mut self) -> Option<Result<u64, Error>> {
        match self {
            Self::Byte(e) => e.pos(),
            _ => None,
        }
    }
}

impl<T> Iterator for ReaderKind<T>
where
    T: Read + Seek,
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
    T: Read + Seek,
{
    /// Get next line (read until `\n`)
    fn next_line(&mut self) -> Option<Result<String, Error>> {
        let mut s = String::new();
        match self.br.read_line(&mut s) {
            Ok(0) => None,
            Err(e) => Some(Err(Error::Io(e))),
            // trim_end to remove the trailing newline.
            _ => Some(Ok(s.trim_end().to_owned())),
        }
    }

    pub fn lang(&self) -> &'static str {
        self.lang
    }

    /// Returns the position in the stream. See [std::io::Seek::stream_position] for more details.
    pub fn pos(&mut self) -> Option<Result<u64, Error>> {
        Some(self.br.stream_position().map_err(Error::Io))
    }

    fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl ByteReader<File> {
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
}

/// Reader that yields sequences of strings
/// that are newline separated.
#[derive(Debug)]
pub struct LineReader<T> {
    path: PathBuf,
    lines: Lines<BufReader<T>>,
    lang: &'static str,
}

impl LineReader<File> {
    pub fn new(src: &Path, lang: &'static str) -> Result<Self, Error> {
        Ok(ByteReader::new(src, lang)?.into())
    }
}

impl<T> From<ByteReader<T>> for LineReader<T>
where
    T: Read + Seek,
{
    fn from(br: ByteReader<T>) -> LineReader<T> {
        LineReader {
            path: br.path().to_owned(),
            lines: br.br.lines(),
            lang: br.lang,
        }
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

impl<T> Iterator for LineReader<T>
where
    T: Read,
{
    type Item = Result<Vec<String>, Error>;
    fn next(&mut self) -> Option<Self::Item> {
        let mut ret = Vec::new();
        while let Some(Ok(sen)) = self.lines.next() {
            //cut at empty line
            if sen.is_empty() {
                return Some(ret.into_iter().collect());
            }
            ret.push(Ok(sen));
        }

        // close eventual last vec
        if ret.is_empty() {
            None
        } else {
            Some(ret.into_iter().collect())
        }
    }
}

impl<T> Iterator for ByteReader<T>
where
    T: Read + Seek,
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
    fn test_iter_bytes() {
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
        let tr = ByteReader {
            path: PathBuf::new(), //empty, for testing
            br,
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
