//! Metadata (WARC Record Headers + offset/nb_sentences).
//!
//! Holds record headers as [String] rather than as [u8],
//! and adds offset and nb_sentences to help retrieve sentences
//! from text file.
//!
//! Also implements [serde::Serialize] and [serde::Deserialize] for JSON serialization.
use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;
use std::convert::TryFrom;
use std::string::FromUtf8Error;

use warc::header::WarcHeader;

/// Holds record headers.
///
/// Each metadata is linked to a specific paragraph/text zone
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Metadata {
    pub headers: HashMap<WarcHeader, String>,
    pub offset: usize,
    pub nb_sentences: usize,
}

impl TryFrom<HashMap<WarcHeader, Vec<u8>>> for Metadata {
    type Error = FromUtf8Error;
    fn try_from(hm: HashMap<WarcHeader, Vec<u8>>) -> Result<Self, Self::Error> {
        let values: Vec<String> = hm
            .values()
            .map(|v| String::from_utf8(v.to_vec()))
            .collect::<Result<Vec<String>, Self::Error>>()?;

        let keys = hm.keys().cloned();
        let headers = keys.into_iter().zip(values.into_iter()).collect();
        Ok(Metadata {
            headers,
            offset: 0,
            nb_sentences: 0,
        })
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialize() {
        let mut headers: HashMap<WarcHeader, String> = HashMap::new();
        headers.insert(WarcHeader::WarcType, "conversion".to_string());
        headers.insert(WarcHeader::ContentLength, "6231".to_string());
        headers.insert(
            WarcHeader::Unknown("warc-identified-content-language".to_string()),
            "zho".to_string(),
        );
        let metadata = Metadata {
            headers,
            offset: 0,
            nb_sentences: 0,
        };

        assert!(serde_json::to_string(&metadata).is_ok());
    }

    #[test]
    fn deserialize() {
        let meta_json = r#"{"headers":{"warc-type":"conversion","content-length":"6231","warc-identified-content-language":"zho"},"offset":0, "nb_sentences": 0}"#;
        let mut headers: HashMap<WarcHeader, String> = HashMap::new();
        headers.insert(WarcHeader::WarcType, "conversion".to_string());
        headers.insert(WarcHeader::ContentLength, "6231".to_string());
        headers.insert(
            WarcHeader::Unknown("warc-identified-content-language".to_string()),
            "zho".to_string(),
        );
        let expected = Metadata {
            headers,
            offset: 0,
            nb_sentences: 0,
        };
        let result: Metadata = serde_json::from_str(&meta_json).unwrap();
        assert_eq!(result, expected);
    }
}
