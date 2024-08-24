use crate::storage::index::generic_key::Comparator;
use std::cmp::Ordering;
use std::marker::PhantomData;

#[derive(Debug, Clone, Copy)]
pub struct IntComparator {
    _marker: PhantomData<()>,
}

impl IntComparator {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl Comparator<i32> for IntComparator {
    /// Compares two integers.
    ///
    /// # Parameters
    /// - `lhs`: Left-hand side integer.
    /// - `rhs`: Right-hand side integer.
    ///
    /// # Returns
    /// - `1` if `lhs` > `rhs`
    /// - `-1` if `lhs` < `rhs`
    /// - `0` if `lhs` == `rhs`
    fn compare(&self, lhs: &i32, rhs: &i32) -> Ordering {
        lhs.cmp(rhs)
    }
}
