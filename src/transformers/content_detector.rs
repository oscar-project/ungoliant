/*! Content annotators.

Adds an `annotation` tag in [Document] depending on possibly harmful/specific content in document.

Currently the approach is to use the [UT1 blocklist](https://dsi.ut-capitole.fr/blacklists/) and to annotate flagged URLs.
 * !*/

use log::info;
use ut1_blocklist::MultipleBlocklist as Blocklist;

use crate::pipelines::oscardoc::types::Document;

use super::Annotate;

pub struct ContentDetector {
    bl: Blocklist,
}

impl ContentDetector {
    /// Create a new [ContentDetector] based on a specified [Blocklist].
    pub fn new(bl: Blocklist) -> Self {
        info!("Creating a new ContentDetector");
        Self { bl }
    }
}

impl Annotate<Document> for ContentDetector {
    /// Checks if domain/url is present in provided blocklist, and adds a tag
    /// corresponding to blocklist kind if true.
    fn annotate(&self, doc: &mut Document) {
        if let Some(url) = doc.url() {
            let categories: Option<Vec<String>> = self
                .bl
                .detect(&url)
                .map(|categories| categories.into_iter().map(String::from).collect());
            doc.metadata_mut().set_categories(categories);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::{HashMap, HashSet},
        path::Path,
    };

    use ut1_blocklist::MultipleBlocklist as Blocklist;
    use warc::WarcHeader;

    use crate::{
        pipelines::oscardoc::types::{Document, Metadata},
        transformers::Annotate,
    };

    use super::ContentDetector;

    fn gen_document(url: &str) -> Document {
        let content = String::new();
        let mut headers = HashMap::new();
        headers.insert(WarcHeader::TargetURI, url.as_bytes().to_vec());
        let metadata = Metadata::default();

        Document::new(content, headers, metadata)
    }

    #[test]
    fn test_annotation() {
        let mut doc = gen_document("https://foo.bar");

        let mut domains = HashMap::new();
        domains.insert("foo.bar".to_string(), vec!["adult".to_string()]);

        let bl = Blocklist::new(domains, HashMap::new());
        let cd = ContentDetector::new(bl);

        cd.annotate(&mut doc);

        assert_eq!(
            doc.metadata().categories(),
            Some(vec!["adult".to_string()]).as_ref()
        );
    }

    #[test]
    fn test_annotation_false() {
        let mut doc = gen_document("https://foo.bar");

        let mut domains = HashMap::new();
        domains.insert("baz.quux".to_string(), vec!["adult".to_string()]);

        let bl = Blocklist::new(domains, HashMap::new());
        let cd = ContentDetector::new(bl);

        cd.annotate(&mut doc);

        assert!(doc.metadata().annotation().is_none());
    }
}
