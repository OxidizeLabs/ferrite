use std::cmp::Ordering;
use std::marker::PhantomData;

/// A generic key used for indexing with opaque data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GenericKey<T, const N: usize> {
    /// The fixed-size array holding the key data.
    data: [u8; N],
    _marker: PhantomData<T>,
}

impl<T, const N: usize> GenericKey<T, N> {
    /// Creates a new `GenericKey` with zeroed data.
    pub fn new() -> Self {
        Self {
            data: [0; N],
            _marker: PhantomData,
        }
    }

    /// Sets the key data from a slice of bytes.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The slice of bytes to set the key from.
    pub fn set_from_bytes(&mut self, bytes: &[u8]) {
        let len = bytes.len().min(N);
        self.data[..len].copy_from_slice(&bytes[..len]);
    }

    /// Returns a reference to the key's byte data.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Comparator for `GenericKey`.
pub struct GenericKeyComparator<T, const N: usize> {
    _marker: PhantomData<T>,
}

impl<T, const N: usize> GenericKeyComparator<T, N> {
    /// Creates a new `GenericKeyComparator`.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    /// Compares two `GenericKey`s.
    ///
    /// # Arguments
    ///
    /// * `lhs` - The left-hand side key.
    /// * `rhs` - The right-hand side key.
    ///
    /// # Returns
    ///
    /// An `Ordering` representing the comparison result.
    pub fn compare(&self, lhs: &GenericKey<T, N>, rhs: &GenericKey<T, N>) -> Ordering {
        lhs.data.cmp(&rhs.data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generic_key_new() {
        let key: GenericKey<i32, 8> = GenericKey::new();
        assert_eq!(key.as_bytes(), [0; 8]);
    }

    #[test]
    fn test_generic_key_set_from_bytes() {
        let mut key: GenericKey<i32, 8> = GenericKey::new();
        key.set_from_bytes(&[1, 2, 3, 4]);
        assert_eq!(key.as_bytes(), [1, 2, 3, 4, 0, 0, 0, 0]);
    }

    #[test]
    fn test_generic_key_set_from_bytes_overflow() {
        let mut key: GenericKey<i32, 4> = GenericKey::new();
        key.set_from_bytes(&[1, 2, 3, 4, 5, 6]);
        assert_eq!(key.as_bytes(), [1, 2, 3, 4]);
    }

    #[test]
    fn test_generic_key_comparator() {
        let comparator = GenericKeyComparator::<i32, 4>::new();

        let mut key1: GenericKey<i32, 4> = GenericKey::new();
        key1.set_from_bytes(&[1, 2, 3, 4]);

        let mut key2: GenericKey<i32, 4> = GenericKey::new();
        key2.set_from_bytes(&[1, 2, 3, 5]);

        let mut key3: GenericKey<i32, 4> = GenericKey::new();
        key3.set_from_bytes(&[1, 2, 3, 4]);

        assert_eq!(comparator.compare(&key1, &key2), Ordering::Less);
        assert_eq!(comparator.compare(&key2, &key1), Ordering::Greater);
        assert_eq!(comparator.compare(&key1, &key3), Ordering::Equal);
    }
}