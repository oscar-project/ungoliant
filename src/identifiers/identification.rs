/*! Identifier trait

All identifiers should implement [Identifier] to be useable in processing and pipelines.
!*/
use std::ops::Deref;

use crate::error::Error;
use fasttext::Prediction;

use oxilangtag::{LanguageTag, LanguageTagParseError};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Identification<T: Deref<Target = str> + Clone> {
    label: LanguageTag<T>,
    prob: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IdentificationSer {
    label: String,
    prob: f32,
}

impl<T> From<Identification<T>> for IdentificationSer
where
    T: Deref<Target = str> + Clone,
{
    fn from(i: Identification<T>) -> Self {
        Self {
            label: i.label.to_string(),
            prob: i.prob,
        }
    }
}
impl TryFrom<IdentificationSer> for Identification<String> {
    type Error = LanguageTagParseError;
    fn try_from(i: IdentificationSer) -> Result<Self, Self::Error> {
        Ok(Self {
            label: LanguageTag::parse(i.label)?,
            prob: i.prob,
        })
    }
}

impl<T: Deref<Target = str> + Clone> Identification<T> {
    pub fn new(label: LanguageTag<T>, prob: f32) -> Self {
        Self { label, prob }
    }
    /// Get a reference to the identification's label.
    pub fn label(&self) -> &LanguageTag<T> {
        &self.label
    }

    /// Get a reference to the identification's prob.
    pub fn prob(&self) -> &f32 {
        &self.prob
    }
}

/// for fasttext2 predictions
impl TryFrom<Prediction> for Identification<String> {
    type Error = LanguageTagParseError;
    fn try_from(prediction: Prediction) -> Result<Self, LanguageTagParseError> {
        // skip __label__
        let label = prediction.label.chars().skip(9).collect::<String>();

        //convert to valid bcp47
        let label = label.replace('_', "-");

        Ok(Self {
            prob: prediction.prob,
            label: LanguageTag::parse_and_normalize(&label)?,
        })
        // debug!("{prediction:?}");
        // Self {
        //     prob: prediction.prob,
        //     label: Lang::from_str(&prediction.label.chars().skip(9).collect::<String>()).unwrap(),
        // }
    }
}

// impl From<fasttext2::Prediction> for Identification {
//     fn from(prediction: fasttext2::Prediction) -> Self {
//         let label = prediction
//             .label()
//             .chars()
//             .skip(9)
//             .collect::<String>(())
//             .unwrap();
//         todo!()
//     }
// }
pub trait Identifier<T: Deref<Target = str> + Clone> {
    /// returns a language identification token (from [crate::lang::LANG]).
    fn identify(&self, sentence: T) -> Result<Option<Identification<T>>, Error>;
}

#[cfg(test)]
mod tests {
    use fasttext::Prediction;

    // use crate::identifiers::tag_convert::{NewTag, OldTag};

    use crate::identifiers::tag_convert::Tag;

    use super::Identification;

    #[test]
    fn test_from_pred() {
        let prob = 1.0f32;
        let label = "__label__en".to_string();
        let p = Prediction { prob, label };

        let id: Identification<String> = Identification::try_from(p.clone()).unwrap();
        assert_eq!(&id.label().to_string(), &"en");
        assert_eq!(id.prob(), &p.prob);
    }

    #[test]
    fn test_old_new_tryfrom() {
        let prob = 1.0f32;
        let label = "__label__en".to_string();
        let old = Prediction { prob, label };

        let old: Identification<String> =
            Identification::new(Tag::new(&old.label).try_into().unwrap(), old.prob);
        Identification::new(Tag::new(&old.label).try_into().unwrap(), old.prob);

        let prob = 1.0f32;
        let label = "__label__eng".to_string();
        let new = Prediction { prob, label };
        let new: Identification<String> =
            Identification::new(Tag::new(&new.label).try_into().unwrap(), new.prob);

        assert_eq!(old.label(), new.label());
    }

    // Uses language tags from fasttext's lid.176.bin
    #[test]
    fn test_bcp47() {
        let model_codes = [
            "en", "ru", "de", "fr", "it", "ja", "es", "ceb", "tr", "pt", "uk", "eo", "pl", "sv",
            "nl", "he", "zh", "hu", "ar", "ca", "fi", "cs", "fa", "sr", "el", "vi", "bg", "ko",
            "no", "mk", "ro", "id", "th", "hy", "da", "ta", "hi", "hr", "sh", "be", "ka", "te",
            "kk", "war", "lt", "gl", "sk", "bn", "eu", "sl", "kn", "ml", "mr", "et", "az", "ms",
            "sq", "la", "bs", "nn", "ur", "lv", "my", "tt", "af", "oc", "nds", "ky", "ast", "tl",
            "is", "ia", "si", "gu", "km", "br", "ba", "uz", "bo", "pa", "vo", "als", "ne", "cy",
            "jbo", "fy", "mn", "lb", "ce", "ug", "tg", "sco", "sa", "cv", "jv", "min", "io", "or",
            "as", "new", "ga", "mg", "an", "ckb", "sw", "bar", "lmo", "yi", "arz", "mhr", "azb",
            "sah", "pnb", "su", "bpy", "pms", "ilo", "wuu", "ku", "ps", "ie", "xmf", "yue", "gom",
            "li", "mwl", "kw", "sd", "hsb", "scn", "gd", "pam", "bh", "mai", "vec", "mt", "dv",
            "wa", "mzn", "am", "qu", "eml", "cbk", "tk", "rm", "os", "vls", "yo", "lo", "lez",
            "so", "myv", "diq", "mrj", "dsb", "frr", "ht", "gn", "bxr", "kv", "sc", "nah", "krc",
            "bcl", "nap", "gv", "av", "rue", "xal", "pfl", "dty", "hif", "co", "lrc", "vep", "tyv",
        ];

        for code in model_codes {
            let pred = Prediction {
                label: "__label__".to_string() + code,
                prob: 1.0f32,
            };

            let id = Identification::try_from(pred);
            assert!(id.is_ok());
        }
    }

    // Uses language tags from fasttext's other langid model
    #[test]
    fn test_bcp47_2() {
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
            let pred = Prediction {
                label: "__label__".to_string() + code,
                prob: 1.0f32,
            };

            let id = Identification::try_from(pred);
            assert!(id.is_ok());
        }
    }
}
