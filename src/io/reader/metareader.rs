use std::fs::File;
use std::io::{BufRead, Lines, Read};

use std::io::BufReader;
use std::path::Path;

use crate::error::Error;
use crate::processing::Metadata;

/// Same implementation of Reader, same new, different iter implementation.
/// This should be doable by defining a trait that implements Iterator.
#[derive(Debug)]
pub struct Reader<T>
where
    T: Read,
{
    // br: BufReader<T>,
    lines: Lines<BufReader<T>>,
    lang: &'static str,
}

pub type MetaReader = Reader<File>;

impl MetaReader {
    pub fn new(src: &Path, lang: &'static str) -> Result<Self, Error> {
        let filename = format!("{}_meta.jsonl", lang);
        let mut src = src.to_path_buf();
        src.push(filename);
        let metahandler = File::open(src)?;
        let br = BufReader::new(metahandler);
        let lines = br.lines();
        Ok(Self { lines, lang })
    }
}

impl<T> Iterator for Reader<T>
where
    T: Read,
{
    type Item = Result<Metadata, Error>;

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
        Some(serde_json::from_str::<Metadata>(&meta_str).map_err(Error::Serde))
    }
}
#[cfg(test)]
mod tests {
    use std::{
        fs::File,
        io::{BufRead, BufReader},
        path::PathBuf,
    };

    use itertools::Itertools;
    use std::io::Cursor;

    use crate::processing::Metadata;

    use super::*;

    fn gen_data() -> String {
        let data = r#"{"headers":{"warc-date":"2021-02-24T18:50:04Z","warc-identified-content-language":"afr","content-type":"text/plain","warc-record-id":"<urn:uuid:4c2d4cbb-24ef-4885-9516-d131fc15af2e>","content-length":"4891","warc-type":"conversion","warc-refers-to":"<urn:uuid:94ff8c3f-838f-44e6-ba2a-a47262025819>","warc-block-digest":"sha1:UMHO34U75QPBAT2DB756CLZYMVGQZC6G","warc-target-uri":"https://foo.bar/baz"},"offset":0,"nb_sentences":2}
{"headers":{"warc-refers-to":"<urn:uuid:8eca168f-1bd7-4ab8-bbe5-4ec64a547f35>","content-length":"5913","warc-target-uri":"http://foo.bar/baz/quux","warc-identified-content-language":"afr","warc-record-id":"<urn:uuid:8f77684e-09d2-48d5-a81b-4117add8614c>","warc-block-digest":"sha1:GAYXLT4D2I3R34IPT4OGVCHWCKNDGK4F","warc-type":"conversion","content-type":"text/plain","warc-date":"2021-02-24T17:08:20Z"},"offset":3,"nb_sentences":2}
{"headers":{"warc-type":"conversion","warc-record-id":"<urn:uuid:610cefa9-de2b-412a-80e1-1ba68ab85e08>","warc-refers-to":"<urn:uuid:beff3c61-4c5c-4bf0-83c8-d9e8ba0ff1f3>","warc-date":"2021-02-24T17:29:45Z","warc-identified-content-language":"afr","content-length":"17550","warc-block-digest":"sha1:QC4BQEBP4SPAX3F555A7TWVU7M3BNBIL","content-type":"text/plain","warc-target-uri":"http://foo.bar.baz/quux"},"offset":6,"nb_sentences":11}
"#.to_string();
        data
    }

    #[test]
    fn test_first() {
        let d = gen_data();
        let c = Cursor::new(d);
        let b = BufReader::new(c);
        let mut mr = Reader {
            lines: b.lines(),
            lang: "en",
        };

        assert!(mr.next().is_some());
    }

    #[test]
    fn test_last() {
        let d = gen_data();
        let c = Cursor::new(d);
        let b = BufReader::new(c);
        let l = b.lines();
        let mut mr = Reader {
            lines: l,
            lang: "en",
        };

        assert!(mr.last().is_some());
    }

    #[test]
    fn test_all() {
        let d = gen_data();
        let c = Cursor::new(d);
        let b = BufReader::new(c);
        let l = b.lines();
        let mut mr = Reader {
            lines: l,
            lang: "en",
        };

        for m in mr {
            println!("{:?}", m);
            assert!(m.is_ok());
        }
    }
}
