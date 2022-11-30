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
        Self { path }
    }

    pub fn build(&self) -> AdultDetector {
        AdultDetector {
            kenlm: KenLM::new(self.path.to_string_lossy(), &Dict::new()),
            pp_thresh: 1000.0,
        }
    }
}
pub struct AdultDetector {
    kenlm: KenLM,
    pp_thresh: f32,
}

impl AdultDetector {
    pub fn new(model_path: &Path, pp_thresh: f32) -> Self {
        //TODO: check existencemodel_path.as_os_str().to_str().unwrap() of path
        let model_path = model_path.as_os_str().to_str().unwrap();
        Self {
            kenlm: KenLM::new(model_path, &Dict::new()),
            pp_thresh,
        }
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

impl Default for AdultDetectorBuilder {
    fn default() -> Self {
        Self {
            path: PathBuf::from("kenlm_big.binary"),
        }
    }
}

impl Default for AdultDetector {
    fn default() -> Self {
        Self {
            kenlm: KenLM::new("kenlm_big.binary", &Dict::new()),
            pp_thresh: 2906f32,
        }
    }
}
#[cfg(test)]
mod test {

    // use ctclib::{Dict, KenLM, Model};
}
