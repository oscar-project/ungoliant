use std::path::{Path, PathBuf};

use ctclib::{Dict, KenLM};

use crate::{pipelines::oscardoc::types::Document, transformers::Annotate};
use log::{debug, info};
use warc::WarcHeader;

pub struct AdultDetectorBuilder {
    path: PathBuf,
}

impl AdultDetectorBuilder {
    pub fn new(path: PathBuf) -> AdultDetectorBuilder {
        debug!("New builder: {:?}", path);
        Self { path }
    }

    pub fn build(&self) -> Result<AdultDetector, std::io::Error> {
        debug!("Building new KenLM from path {:?}", self.path);
        if !self.path.exists() {
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("{:?} not found.", self.path),
            ))
        } else {
            Ok(AdultDetector {
                kenlm: KenLM::new(&self.path, &Dict::new())?,
                pp_thresh: 1000.0,
            })
        }
    }
}
pub struct AdultDetector {
    kenlm: KenLM,
    pp_thresh: f32,
}

impl AdultDetector {
    pub fn new(model_path: &Path, pp_thresh: f32) -> Result<Self, std::io::Error> {
        //TODO: check existencemodel_path.as_os_str().to_str().unwrap() of path
        Ok(Self {
            kenlm: KenLM::new(model_path, &Dict::new())?,
            pp_thresh,
        })
    }
}

impl Annotate<Document> for AdultDetector {
    fn annotate(&self, doc: &mut Document) {
        let content = doc.content().replace('\n', " ");
        doc.metadata_mut()
            .set_harmful_pp(Some(self.kenlm.perplexity(&content)));
        // if self.kenlm.perplexity(&content) > self.pp_thresh {
        //     //TODO: add_annotation rather than set
        //     info!(
        //         "Document is adult! {}",
        //         String::from_utf8_lossy(doc.warc_headers().get(&WarcHeader::RecordID).unwrap())
        //     );
        //     debug!("{}", doc.content());
        //     doc.metadata_mut().set_annotation("adult_pp".to_string());
        // }
    }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;

    use super::AdultDetectorBuilder;

    // use ctclib::{Dict, KenLM, Model};
    #[test]
    fn test_nonexisting() {
        let adb = AdultDetectorBuilder::new(PathBuf::from("fezlfzej"));
        assert!(adb.build().is_err());
    }

    #[test]
    fn test_existing_valid() {
        let adb = AdultDetectorBuilder::new(PathBuf::from("res/kenlm/en.arpa"));
        assert!(adb.build().is_ok());
    }

    // See https://github.com/Uinelj/ctclib/issues/1
    // #[test]
    // fn test_existing_invalid() {
    //     let adb = AdultDetectorBuilder::new(PathBuf::from("res/kenlm/overfit_bugged.arpa"));
    //     assert!(adb.build().is_err());
    // }
}
