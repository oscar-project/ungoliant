use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

use warc::header::WarcHeader;

/// Holds record headers.
///
/// Each metadata is linked to a specific paragraph/text zone
/// TODO enhance doc here to explain usage
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Metadata {
    pub headers: HashMap<WarcHeader, String>,
    pub offset: usize,
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
        let metadata = Metadata { headers, offset: 0 };

        assert!(serde_json::to_string(&metadata).is_ok());
    }

    #[test]
    fn deserialize() {
        let meta_json = r#"{"headers":{"warc-type":"conversion","content-length":"6231","warc-identified-content-language":"zho"},"offset":0}"#;
        let mut headers: HashMap<WarcHeader, String> = HashMap::new();
        headers.insert(WarcHeader::WarcType, "conversion".to_string());
        headers.insert(WarcHeader::ContentLength, "6231".to_string());
        headers.insert(
            WarcHeader::Unknown("warc-identified-content-language".to_string()),
            "zho".to_string(),
        );
        let expected = Metadata { headers, offset: 0 };
        let result: Metadata = serde_json::from_str(&meta_json).unwrap();
        assert_eq!(result, expected);
    }
}
