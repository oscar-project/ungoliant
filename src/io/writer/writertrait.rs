pub trait WriterTrait<T> {
    fn write(&mut self, vals: &[T]) -> Result<(), Error>;
    fn write_single(&mut self, val: &T) -> Result<(), Error>;
}
