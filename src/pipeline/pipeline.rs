use crate::error::Error;

pub trait Pipeline<T> {
    fn run(&self) -> Result<T, Error>;
}
