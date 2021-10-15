/*! Content annotators.

Adds an `annotation` tag in [Document] depending on possibly harmful/specific content in document.

Currently the approach is to use the [UT1 blocklist](https://dsi.ut-capitole.fr/blacklists/) and to annotate flagged URLs.
 * !*/
use std::str::FromStr;

use ut1_blocklist::{self, Blocklist};

use crate::{error::Error, pipelines::oscardoc::types::Document};
use url::Url;

use super::transform::Transform;

pub struct ContentDetector<'a> {
    bl: Blocklist<'a>,
}

impl<'a> ContentDetector<'a> {
    pub fn new(bl: Blocklist<'a>) -> Self {
        Self { bl }
    }

    pub fn with_defaults() -> Result<Self, Error> {
        let bl = Blocklist::with_defaults()?;
        Ok(Self { bl })
    }

    /// Attempt to extract url from [Document].
    /// Returns [None] if no valid URL is found.
    fn parse_url(doc: &Document) -> Option<Url> {
        doc.warc_headers()
            .get(&warc::WarcHeader::TargetURI)
            .map(|x| String::from_utf8_lossy(x))
            .and_then(|x| Url::from_str(&x).ok())
    }
}

impl<'a> Transform for ContentDetector<'a> {
    fn transform_own(&self, mut doc: Document) -> Document {
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
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        path::Path,
    };

    use ut1_blocklist::Blocklist;
    use warc::WarcHeader;

    use crate::{
        pipelines::oscardoc::types::{Document, Metadata},
        transformers::Transform,
    };

    use super::ContentDetector;

    fn gen_document(url: &str) -> Document {
        let content = String::new();
        let mut headers = HashMap::new();
        headers.insert(WarcHeader::TargetURI, url.as_bytes().to_vec());
        let metadata = Metadata::default();
        let d = Document::new(content, headers, metadata);

        d
    }

    #[test]
    fn test_init_defaults() {
        let default_path = Path::new("./ut1-blacklists/blacklists/");

        let cd = ContentDetector::with_defaults();
        if default_path.exists() {
            assert!(cd.is_ok());
        } else {
            assert!(cd.is_err());
        }
    }
    #[test]
    fn test_annotation() {
        let doc = gen_document("https://foo.bar");

        let mut domains = HashSet::new();
        domains.insert("foo.bar".to_string());

        let bl = Blocklist::new("adult", domains, HashSet::new());
        let cd = ContentDetector::new(bl);

        let doc = cd.transform_own(doc);

        assert_eq!(doc.metadata().annotation(), Some(&"adult".to_string()));
    }

    #[test]
    fn test_annotation_false() {
        let doc = gen_document("https://foo.bar");

        let mut domains = HashSet::new();
        domains.insert("baz.quux".to_string());

        let bl = Blocklist::new("adult", domains, HashSet::new());
        let cd = ContentDetector::new(bl);

        let doc = cd.transform_own(doc);

        assert!(doc.metadata().annotation().is_none());
    }
}
