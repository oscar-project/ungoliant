/*! Locality sensitive hashing !*/

use oscar_io::oscar_doc::WarcHeaders;
use tlsh::{BucketKind, ChecksumKind, TlshBuilder};

use crate::pipelines::oscardoc::types::Document;
use warc::WarcHeader;

use super::Annotate;
use log::warn;
pub struct LSH {
    builder: TlshBuilder,
}

impl LSH {
    pub fn new(builder: TlshBuilder) -> Self {
        Self { builder }
    }
}
impl Annotate<Document> for LSH {
    fn annotate(&self, doc: &mut Document) {
        let mut builder = self.builder.clone();
        builder.update(doc.content().as_bytes());
        match builder.build() {
            Ok(hash) => {
                let annotation = format!("tlsh:{}", hash.hash());
                doc.metadata_mut().add_annotation(annotation);
            }
            Err(e) => warn!(
                "Could not compute a hash for document {:?}: {:?}",
                String::from_utf8_lossy(
                    doc.warc_headers()
                        .get(&WarcHeader::RecordID)
                        .unwrap_or(&vec![])
                ),
                e
            ),
        }
    }
}

impl Default for LSH {
    fn default() -> Self {
        let mut builder = TlshBuilder::new(
            BucketKind::Bucket256,
            ChecksumKind::ThreeByte,
            tlsh::Version::Version4,
        );

        Self::new(builder)
    }
}
#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use tlsh::{BucketKind, ChecksumKind, TlshBuilder};

    use crate::{
        pipelines::oscardoc::types::{Document, Metadata},
        transformers::Annotate,
    };

    use super::LSH;

    #[test]
    fn test_annotate() {
        let s = r#"cvqlmd,cpqlzec;)à"ç!(àb"(!uyiuegfbnsoc,)az"à(!ç"#.to_string();
        let mut doc = Document::new(s, HashMap::new(), Metadata::default());
        let lsh = LSH::default();
        lsh.annotate(&mut doc);

        let annotation = &doc.metadata().annotation().unwrap()[0];
        assert!(annotation.contains("tlsh:"));
    }

    #[test]
    fn test_no_hash() {
        let s = "oooooooooooooooooooooooooooooooooooooooooooooooooo".to_string();
        let mut doc = Document::new(s, HashMap::new(), Metadata::default());
        let lsh = LSH::default();
        lsh.annotate(&mut doc);

        assert!(doc.metadata().annotation().is_none());
    }

    #[test]
    fn test_too_short() {
        let s = "a".to_string();
        let mut doc = Document::new(s, HashMap::new(), Metadata::default());
        let lsh = LSH::default();
        lsh.annotate(&mut doc);

        assert!(doc.metadata().annotation().is_none());
    }
    #[test]
    fn test_tlsh() {
        let s1 = "Le Mallorquín ou Majorquin (Cavall Mallorquí en catalan) est une race de chevaux de selle à la robe noire, 
        autochtone de Majorque, l'une des îles Baléares en Espagne, à laquelle il doit son nom. 
        Il est très proche du cheval Minorquin, et souvent confondu avec lui. 
        Vraisemblablement issu de chevaux celtiques et notamment du cheval catalan, il est introduit sur l'île de Majorque avec de nombreux croisements au XIXe siècle.
        La motorisation ayant raison de son développement, il manque de disparaître dans les années 1970.".to_string();

        let s2 = s1.clone();

        let annotator = LSH::default();
        let mut doc1 = Document::new(s1, HashMap::new(), Metadata::default());
        let mut doc2 = Document::new(s2, HashMap::new(), Metadata::default());
        annotator.annotate(&mut doc1);
        annotator.annotate(&mut doc2);

        let hash1 = &doc1.metadata().annotation().unwrap()[0];
        let hash2 = &doc2.metadata().annotation().unwrap()[0];

        assert_eq!(hash1, hash2);
    }
}
