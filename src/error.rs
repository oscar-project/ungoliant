#[derive(Debug)]
pub enum Error {
    Io(std::io::Error),
    UnknownLang(String),
    Custom(String),
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Error {
        Error::Io(e)
    }
}

impl From<String> for Error {
    fn from(s: String) -> Error {
        Error::Custom(s)
    }
}
