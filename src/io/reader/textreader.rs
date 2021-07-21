use std::{
    fs::File,
    io::{BufRead, BufReader, Lines, Read},
    path::Path,
};

use crate::error::Error;

/// Reader that yields sequences of strings
/// that are newline separated.
#[derive(Debug)]
pub struct Reader<T>
where
    T: Read,
{
    // br: BufReader<T>,
    lines: Lines<BufReader<T>>,
    pub lang: &'static str,
}

pub type TextReader = Reader<File>;

impl TextReader {
    pub fn new(src: &Path, lang: &'static str) -> Result<Self, Error> {
        let filename = format!("{}.txt", lang);
        let mut src = src.to_path_buf();
        src.push(filename);
        let texthandler = File::open(src)?;
        let br = BufReader::new(texthandler);
        let lines = br.lines();
        Ok(Self { lines, lang })
    }
}

impl<T> Iterator for Reader<T>
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
        let tr = Reader {
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
        let tr = Reader {
            lines: br.lines(),
            lang: "en",
        };
        for (res, exp) in tr.zip(expected.iter()) {
            let res = res.unwrap();
            assert_eq!(&res, exp);
        }
    }
}
