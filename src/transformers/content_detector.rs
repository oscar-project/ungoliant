use std::str::FromStr;

use ut1_blocklist::{self, Blocklist};
use warc::{BufferedBody, Record};

use crate::{error::Error, pipeline::Document};
use url::Url;

pub struct ContentDetector<'a> {
    bl: Blocklist<'a>,
}

impl<'a> ContentDetector<'a> {
    // pub fn transform(&self, doc: &mut Document) -> Result<(), Error> {
    pub fn transform(&self, doc: &mut Document) {
        let url = String::from_utf8_lossy(
            doc.warc_headers()
                .get(&warc::WarcHeader::TargetURI)
                .unwrap(),
        );

        let url = Url::from_str(&url).unwrap();

        if self.bl.detect_domain(&url) {
            doc.metadata_mut()
                .set_annotation(Some(self.bl.kind().to_string()));
        }
    }

    fn parse_url(doc: &Document) -> Option<Url> {
        doc.warc_headers()
            .get(&warc::WarcHeader::TargetURI)
            .map(|x| String::from_utf8_lossy(x))
            .and_then(|x| Url::from_str(&x).ok())
    }
    pub fn transform_own(&self, mut doc: Document) -> Document {
        // attempt to get a valid url
        let url = Self::parse_url(&doc);

        // if we were successful, detect domain and url
        if let Some(valid_url) = url {
            if self.bl.detect_domain(&valid_url) || self.bl.detect_url(&valid_url) {
                doc.metadata_mut()
                    .set_annotation(Some(self.bl.kind().to_string()));
            }
        }

        doc
    }

    pub fn get_annotation(&self, record: &Record<BufferedBody>) -> Option<String> {
        if let Some(url) = record.header(warc::WarcHeader::TargetURI) {
            let url = String::from_utf8_lossy(url.as_bytes());
            let url = Url::from_str(&url).unwrap();
            if self.bl.detect_url(&url) {
                Some(self.bl.kind().to_string())
            } else {
                None
            }
        } else {
            None
        }
    }
}

impl<'a> Default for ContentDetector<'a> {
    fn default() -> Self {
        Self {
            bl: Default::default(),
        }
    }
}
