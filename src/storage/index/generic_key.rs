use std::cmp::Ordering;
use std::marker::PhantomData;
use std::convert::TryInto;
use crate::catalog::schema::Schema;
use crate::types_db::value::Value;

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

    /// Returns a clone of the underlying value.
    pub fn get_value(&self) -> T
    where
        T: From<[u8; N]>,
    {
        T::from(self.data)
    }

    /// Converts the generic key into a `Value` using the provided schema and column index.
    ///
    /// Mimics the C++ `ToValue` method:
    /// - If the column is inlined, the data is taken directly from the key's data at the column's offset.
    /// - Otherwise, it reads a 4-byte offset out of the key's data to find the actual value location.
    pub fn to_value(&self, schema: &Schema, column_idx: u32) -> Value {
        // Convert the column index to usize and assume it's valid.
        let column = schema.get_column(column_idx as usize)
            .expect("Invalid column index in Schema");
        let column_type = column.get_type();
        if column.is_inlined() {
            let offset = column.get_offset() as usize;
            let data_slice = &self.data[offset..];
            Value::deserialize_from(data_slice, column_type)
        } else {
            let offset = column.get_offset() as usize;
            // Read a 4-byte offset (i32) from the key's data.
            let raw_offset_bytes = &self.data[offset..offset + 4];
            let relative_offset = i32::from_ne_bytes(
                raw_offset_bytes.try_into().expect("Invalid slice length")
            ) as usize;
            let data_slice = &self.data[relative_offset..];
            Value::deserialize_from(data_slice, column_type)
        }
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
