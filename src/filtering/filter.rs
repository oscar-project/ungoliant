//! Filtering traits.

/// immutable, pure filter (2 successive equal inputs -> 2 equal outputs)
pub trait Filter<T>: Default {
    fn detect(&self, item: T) -> bool;
}

/// mutable filter (that holds state).
/// Note that the function name is different,
/// Because some filters may be able to use both traits
/// (it is possible to keep same naming but the ergonomics are weird)
pub trait FilterMut<T>: Default {
    fn detect_mut(&mut self, item: T) -> bool;
}
