/// Function object that returns > 0 if lhs > rhs, < 0 if lhs < rhs,
/// = 0 if lhs == rhs.
#[derive(Debug, Clone, Copy)]
pub struct GenericComparator;

impl GenericComparator {
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
    pub fn compare(&self, lhs: i32, rhs: i32) -> i32 {
        match lhs.cmp(&rhs) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Greater => 1,
            std::cmp::Ordering::Equal => 0,
        }
    }
}
