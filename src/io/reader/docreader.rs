/*! Oscar Schema v2 compatible reader.
 * !*/
use std::fs::File;
use std::io::{BufRead, Lines, Read};

use std::io::BufReader;
use std::path::Path;

use crate::error::Error;
use crate::pipelines::oscardoc::types::Document;

/// Same implementation of Reader, same new, different iter implementation.
/// This should be doable by defining a trait that implements Iterator.
#[derive(Debug)]
pub struct Reader<T>
where
    T: Read,
{
    lines: Lines<BufReader<T>>,
}

pub type DocReader = Reader<File>;

impl DocReader {
    pub fn from_path(src: &Path) -> Result<Self, Error> {
        let metahandler = File::open(src)?;
        let br = BufReader::new(metahandler);
        let lines = br.lines();
        Ok(Self { lines })
    }
}

impl<T> Iterator for Reader<T>
where
    T: Read,
{
    type Item = Result<Document, Error>;

    /// iterates over metadata entries
    fn next(&mut self) -> Option<Self::Item> {
        let meta_str = self.lines.next();

        let meta_str = match meta_str {
            // check for special line cases
            Some(Ok(meta_str)) => match meta_str.as_str() {
                "[" => {
                    // if line begin, take next line
                    // if next line has some error, give up and return
                    if let Some(Ok(s)) = self.lines.next() {
                        s
                    } else {
                        return Some(Err(Error::Custom("metadata format error.".to_string())));
                    }
                }
                "]" => return None, //if end of JSON array, return None
                s => s.to_string(),
            },
            Some(Err(e)) => return Some(Err(Error::Io(e))),
            None => return None,
        };

        //parsing
        Some(serde_json::from_str::<Document>(&meta_str).map_err(Error::Serde))
    }
}
#[cfg(test)]
mod tests {
    use std::io::{BufRead, BufReader};

    use std::io::Cursor;

    use super::*;

    fn gen_data() -> String {
        let doc = r#"{
            "content":"foo bar\nbaz quux",
            "warc_headers":{
                "warc-date":"2021-02-24T18:50:04Z",
                "warc-identified-content-language":"afr",
                "content-type":"text/plain",
                "warc-record-id":"<urn:uuid:4c2d4cbb-24ef-4885-9516-d131fc15af2e>",
                "content-length":"4891",
                "warc-type":"conversion",
                "warc-refers-to":"<urn:uuid:94ff8c3f-838f-44e6-ba2a-a47262025819>",
                "warc-block-digest":"sha1:UMHO34U75QPBAT2DB756CLZYMVGQZC6G",
                "warc-target-uri":"https://foo.bar/baz"
            },
            "metadata":{"identification": {"label":"fr","prob":0.96456933},
                "annotation":["short_sentences"],
                "sentence_identifications":[{"label":"en","prob":0.87083143},null,null,null,null,{"label":"fr","prob":0.9551965}]
            }
        }
"#.to_string();

        let mut doc_no_newline: String = doc
            .lines()
            .into_iter()
            .map(|line| line.trim_matches(char::is_whitespace))
            .collect();
        let mut ret = String::new();
        for _ in 0..10 {
            ret.push_str(&doc_no_newline);
            ret.push('\n');
        }

        println!("{}", ret);
        ret
    }

    #[test]
    fn test_first() {
        let d = gen_data();
        let c = Cursor::new(d);
        let b = BufReader::new(c);
        let mut mr = Reader { lines: b.lines() };

        let n = mr.next();
        println!("{:#?}", n);
        assert!(mr.next().unwrap().is_ok());
    }

    #[test]
    fn test_last() {
        let d = gen_data();
        let c = Cursor::new(d);
        let b = BufReader::new(c);
        let l = b.lines();
        let mr = Reader { lines: l };

        assert!(mr.last().unwrap().is_ok());
    }

    #[test]
    fn test_all() {
        let d = gen_data();
        let c = Cursor::new(d);
        let b = BufReader::new(c);
        let l = b.lines();
        let mr = Reader { lines: l };

        for m in mr {
            println!("{:?}", m);
            assert!(m.is_ok());
        }
    }
}
