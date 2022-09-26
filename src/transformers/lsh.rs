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
    use tlsh::{BucketKind, ChecksumKind, TlshBuilder};

    #[test]
    fn test_tlsh() {
        let s1 = "fooooooooooooooooooooooooooooooooooooooooooooooooo";
        let s1 = r#"cvqlmd,cpqlzec;)à"ç!(àb"(!uyiuegfbnsoc,)az"à(!ç"#;

        let mut builder = TlshBuilder::new(
            BucketKind::Bucket256,
            ChecksumKind::ThreeByte,
            tlsh::Version::Version4,
        );
        builder.update(s1.as_bytes());
        let tlsh1 = builder.build().unwrap();

        let s2 = "Le Mallorquín ou Majorquin (Cavall Mallorquí en catalan) est une race de chevaux de selle à la robe noire, autochtone de Majorque, l'une des îles Baléares en Espagne, à laquelle il doit son nom. Il est très proche du cheval Minorquin, et souvent confondu avec lui. Vraisemblablement issu de chevaux celtiques et notamment du cheval catalan, il est introduit sur l'île de Majorque avec de nombreux croisements au XIXe siècle. La motorisation ayant raison de son développement, il manque de disparaître dans les années 1970.

        aUne association d'éleveurs se mobilise en 1981 pour sauver la race, et obtient l'ouverture d'un stud-book en 1988. Il existe moins de 400 individus Mallorquíns recensés en 2012, mais leur utilisation dans les loisirs équestres les préserve désormais de l'extinction. Sobre et rustique, de taille moyenne et de constitution raffinée, le cheval Mallorquín reste essentiellement élevé à Majorque. ";
        let mut builder = TlshBuilder::new(
            BucketKind::Bucket256,
            ChecksumKind::ThreeByte,
            tlsh::Version::Version4,
        );
        builder.update(s2.as_bytes());
        let tlsh2 = builder.build().unwrap();

        // Calculate diff between s1 & s2, including length difference.
        println!("{}", tlsh1.diff(&tlsh2, true));
        // Calculate diff between s1 & s2, excluding length difference.
        println!("{}", tlsh1.diff(&tlsh2, false));
    }
}
