//! Error enum
use std::string::FromUtf8Error;

use avro_rs::AvroResult;

use crate::pipelines::oscardoc::types::IncompleteLocation;

#[derive(Debug)]
#[allow(dead_code)]
pub enum Error {
    Io(std::io::Error),
    Warc(warc::Error),
    UnknownLang(String),
    MetadataConversion(FromUtf8Error),
    Custom(String),
    Serde(serde_json::Error),
    Glob(glob::GlobError),
    GlobPattern(glob::PatternError),
    Ut1(ut1_blocklist::Error),
    FastText(String),
    IncompleteLocation(IncompleteLocation),
    AvroError(avro_rs::Error),
}

impl From<avro_rs::Error> for Error {
    fn from(v: avro_rs::Error) -> Self {
        Self::AvroError(v)
    }
}

impl From<ut1_blocklist::Error> for Error {
    fn from(v: ut1_blocklist::Error) -> Self {
        Self::Ut1(v)
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<glob::GlobError> for Error {
    fn from(e: glob::GlobError) -> Error {
        Error::Glob(e)
    }
}

impl From<glob::PatternError> for Error {
    fn from(e: glob::PatternError) -> Error {
        Error::GlobPattern(e)
    }
}
impl From<warc::Error> for Error {
    fn from(e: warc::Error) -> Error {
        Error::Warc(e)
    }
}
impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::Custom(s)
    }
}

impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Error {
        Error::MetadataConversion(e)
    }
}

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::Serde(e)
    }
}
