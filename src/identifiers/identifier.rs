/*! old style Identifier trait

All identifiers should implement [Identifier] to be useable in processing and pipelines.
!*/
use std::str::FromStr;

use crate::{error::Error, lang::Lang};
use fasttext::Prediction;
use log::debug;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
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
        debug!("{prediction:?}");
        Self {
            prob: prediction.prob,
            label: Lang::from_str(&prediction.label.chars().skip(9).collect::<String>()).unwrap(),
        }
    }
}
pub trait Identifier<T> {
    /// returns a language identification token (from [crate::lang::LANG]).
    fn identify(&self, sentence: T) -> Result<Option<Identification>, Error>;
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

    #[test]
    fn test_bcp47() {
        use oxilangtag::LanguageTag;

        let model_codes = vec![
            "abk", "ace_Arab", "ace_Latn", "ady", "afr", "aka", "alt", "amh", "ara_Arab",
            "ara_Latn", "arn", "asm", "ast", "awa", "ayr", "azb", "azj", "bak", "bam", "ban",
            "bel", "bem", "ben", "bho", "bis", "bjn_Arab", "bjn_Latn", "bod", "bos", "bug", "bul",
            "bxr", "cat", "ceb", "ces", "che", "chv", "cjk", "ckb", "crh_Latn", "cym", "dan",
            "deu", "dik", "diq", "dyu", "dzo", "ell", "eng", "epo", "est", "eus", "ewe", "ewo",
            "fao", "fas", "fij", "fin", "fon", "fra", "fur", "fuv", "gla", "gle", "glg", "gom",
            "grn", "guj", "hat", "hau", "heb", "hin", "hne", "hrv", "hun", "hye", "ibo", "ilo",
            "ind", "isl", "ita", "jav", "jpn", "kab", "kac", "kal", "kam", "kan", "kas_Arab",
            "kas_Deva", "kat", "kau_Arab", "kau_Latn", "kaz", "kbp", "kea", "khm", "kik", "kin",
            "kir", "kmb", "kon", "kor", "krc", "kur", "lao", "lav", "lij", "lim", "lin", "lit",
            "lmo", "ltg", "ltz", "lua", "lug", "luo", "lus", "mag", "mai", "mal", "mar",
            "min_Latn", "mkd", "mlg", "mlt", "mni_Mtei", "mon", "mos", "mri", "msa", "mya", "nav",
            "nia", "nld", "nno", "nob", "npi", "nso", "nus", "nya", "oci", "orm", "ory", "oss",
            "pag", "pan", "pap", "pcm", "pol", "por", "prs", "pus", "que", "roh", "ron", "run",
            "rus", "sag", "san", "sat", "scn", "shn", "sin", "slk", "slv", "smo", "sna", "snd",
            "som", "sot", "spa", "sqi", "srd", "srp_Cyrl", "ssw", "sun", "swe", "swh", "szl",
            "tah", "tam", "tat_Cyrl", "tel", "tgk", "tgl", "tha", "tir", "tmh_Latn", "tmh_Tfng",
            "ton", "tpi", "tsn", "tso", "tuk", "tum", "tur", "twi", "tzm", "udm", "uig", "ukr",
            "umb", "urd", "uzb", "vec", "vie", "war", "wes", "wol", "xho", "xmf", "yid", "yor",
            "yue", "zho_Hans", "zho_Hant", "zul",
        ];

        for code in model_codes {
            let code = code.replace('_', "-");
            let lt = LanguageTag::parse(code.as_str());
            match lt {
                Ok(c) => {
                    println!("{c:?}");
                    println!("{:?}", c.script())
                }
                Err(_) => println!("{code} not parseable"),
            }
        }
    }
}
