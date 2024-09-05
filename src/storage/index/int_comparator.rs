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

impl IntComparator {
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
    fn compare(&self, lhs: i32, rhs: i32) -> Ordering {
        lhs.cmp(&rhs)
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_int_comparator() {
        let comparator = IntComparator::new();

        assert_eq!(comparator.compare(1, 2), Ordering::Less);
        assert_eq!(comparator.compare(2, 1), Ordering::Greater);
        assert_eq!(comparator.compare(3, 3), Ordering::Equal);
    }
}
