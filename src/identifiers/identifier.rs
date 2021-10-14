/*! Identifier trait

All identifiers should implement [Identifier] to be useable in processing and pipelines.
!*/
use std::str::FromStr;

use fasttext::Prediction;

use crate::{error::Error, lang::Lang};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(from = "IdentificationSer", into = "IdentificationSer")]
pub struct Identification {
    label: Lang,
    prob: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdentificationSer {
    label: String,
    prob: f32,
}

impl From<Identification> for IdentificationSer {
    fn from(i: Identification) -> Self {
        Self {
            label: i.label.to_string(),
            prob: i.prob,
        }
    }
}
impl From<IdentificationSer> for Identification {
    fn from(i: IdentificationSer) -> Self {
        Self {
            label: Lang::from_str(&i.label).unwrap(),
            prob: i.prob,
        }
    }
}

impl Identification {
    pub fn new(label: Lang, prob: f32) -> Self {
        Self { label, prob }
    }
    /// Get a reference to the identification's label.
    pub fn label(&self) -> &Lang {
        &self.label
    }

    /// Get a reference to the identification's prob.
    pub fn prob(&self) -> &f32 {
        &self.prob
    }
}

impl From<Prediction> for Identification {
    fn from(prediction: Prediction) -> Self {
        Self {
            prob: prediction.prob,
            label: Lang::from_str(&prediction.label.chars().skip(9).collect::<String>()).unwrap(),
        }
    }
}
pub trait Identifier {
    /// returns a language identification token (from [crate::lang::LANG]).
    fn identify(&self, sentence: &str) -> Result<Option<Identification>, Error>;
}

#[cfg(test)]
mod tests {
    use fasttext::Prediction;

    use super::Identification;

    #[test]
    fn test_from_pred() {
        let prob = 1.0f32;
        let label = "__label__en".to_string();
        let p = Prediction { prob, label };

        let id = Identification::from(p.clone());
        assert_eq!(&id.label().to_string(), &"en");
        assert_eq!(id.prob(), &p.prob);
    }
}
