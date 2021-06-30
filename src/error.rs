#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    Warc(warc::Error),
    UnknownLang(String),
    Custom(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e)
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
