//! Pipeline trait.
use crate::error::Error;

/// This trait must be implemented for each Pipeline,
/// and is generic over the return type so that
/// any custom pipeline that needs a return type can use the
/// trait aswell.
///
/// TODO wiki/page about creating a new pipeline
pub trait Pipeline<T> {
    fn run(&self) -> Result<T, Error>;
}
