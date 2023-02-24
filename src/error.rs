//! Error enum
use std::string::FromUtf8Error;

use oxilangtag::LanguageTagParseError;

use crate::pipelines::oscardoc::types::IncompleteLocation;

#[derive(Debug)]
#[allow(dead_code)]
#[cfg(not(tarpaulin_include))]
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
    Languagetag(LanguageTagParseError),
    IncompleteLocation(IncompleteLocation),
    Avro(avro_rs::Error),
    Csv(csv::Error),
    OscarIo(oscar_io::Error),
}

#[cfg(not(tarpaulin_include))]
impl From<oscar_io::Error> for Error {
    fn from(v: oscar_io::Error) -> Self {
        Self::OscarIo(v)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<LanguageTagParseError> for Error {
    fn from(v: LanguageTagParseError) -> Self {
        Self::Languagetag(v)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<csv::Error> for Error {
    fn from(v: csv::Error) -> Self {
        Self::Csv(v)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<avro_rs::Error> for Error {
    fn from(v: avro_rs::Error) -> Self {
        Self::Avro(v)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<ut1_blocklist::Error> for Error {
    fn from(v: ut1_blocklist::Error) -> Self {
        Self::Ut1(v)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<glob::GlobError> for Error {
    fn from(e: glob::GlobError) -> Error {
        Error::Glob(e)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<glob::PatternError> for Error {
    fn from(e: glob::PatternError) -> Error {
        Error::GlobPattern(e)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<warc::Error> for Error {
    fn from(e: warc::Error) -> Error {
        Error::Warc(e)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::Custom(s)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<FromUtf8Error> for Error {
    fn from(e: FromUtf8Error) -> Error {
        Error::MetadataConversion(e)
    }
}

#[cfg(not(tarpaulin_include))]
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Error {
        Error::Serde(e)
    }
}
