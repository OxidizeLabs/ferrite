use crate::catalogue::schema::Schema;
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Value;
use std::cmp::Ordering;
use std::fmt;
use std::sync::Arc;

/// A generic key used for indexing with opaque data.
#[derive(Clone)]
pub struct GenericKey<const KEY_SIZE: usize> {
    data: [u8; KEY_SIZE],
}

impl<const KEY_SIZE: usize> GenericKey<KEY_SIZE> {
    /// Initializes a new GenericKey.
    pub fn new() -> Self {
        Self {
            data: [0; KEY_SIZE],
        }
    }

    /// Sets the key data from a tuple.
    ///
    /// # Parameters
    /// - `tuple`: The tuple to set the key from.
    pub fn set_from_key(&mut self, tuple: &Tuple) {
        self.data.fill(0);
        let tuple_data = tuple.get_data();
        let len = tuple_data.len().min(KEY_SIZE);
        self.data[..len].copy_from_slice(&tuple_data[..len]);
    }

    /// Sets the key data from an integer.
    ///
    /// NOTE: For test purpose only.
    ///
    /// # Parameters
    /// - `key`: The integer to set the key from.
    pub fn set_from_integer(&mut self, key: i64) {
        self.data.fill(0);
        let key_bytes = key.to_le_bytes();
        self.data[..size_of::<i64>()].copy_from_slice(&key_bytes);
    }

    /// Converts the key to a value.
    ///
    /// # Parameters
    /// - `schema`: The schema of the key.
    /// - `column_idx`: The index of the column in the schema.
    ///
    /// # Returns
    /// The value of the key.
    pub fn to_value(&self, schema: &Schema, column_idx: usize) -> Value {
        let col = schema.get_column(column_idx).unwrap();
        let column_type = col.get_type();
        let data_ptr = if col.is_inlined() {
            &self.data[col.get_offset()..]
        } else {
            let offset = i32::from_le_bytes(
                self.data[col.get_offset()..col.get_offset() + 4]
                    .try_into()
                    .unwrap(),
            );
            &self.data[offset as usize..]
        };
        Value::deserialize_from(data_ptr, column_type)
    }

    /// Converts the key to a string.
    ///
    /// NOTE: For test purpose only.
    ///
    /// # Returns
    /// The string representation of the key.
    pub fn to_string(&self) -> i64 {
        i64::from_le_bytes(self.data[..size_of::<i64>()].try_into().unwrap())
    }

    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut key = Self::new();
        key.set_from_key_slice(bytes);
        key
    }

    pub fn set_from_key_slice(&mut self, key: &[u8]) {
        self.data.fill(0);
        let len = key.len().min(KEY_SIZE);
        self.data[..len].copy_from_slice(&key[..len]);
    }
}

impl<const KEY_SIZE: usize> fmt::Debug for GenericKey<KEY_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl<const KEY_SIZE: usize> fmt::Display for GenericKey<KEY_SIZE> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

pub trait Comparator<K> {
    fn compare(&self, lhs: &K, rhs: &K) -> Ordering;
}

/// Function object that returns true if lhs < rhs, used for trees.
#[derive(Debug, Clone)]
pub struct GenericComparator {
    key_schema: Arc<Schema>,
}

impl GenericComparator {
    pub fn new(key_schema: Arc<Schema>) -> Self {
        Self { key_schema }
    }

    pub fn compare<const KEY_SIZE: usize>(
        &self,
        lhs: &GenericKey<KEY_SIZE>,
        rhs: &GenericKey<KEY_SIZE>,
    ) -> Ordering {
        let column_count = self.key_schema.get_column_count();

        for i in 0..column_count {
            let lhs_value = lhs.to_value(&self.key_schema, i as usize);
            let rhs_value = rhs.to_value(&self.key_schema, i as usize);

            if lhs_value.compare_less_than(&rhs_value) == CmpBool::CmpTrue {
                return Ordering::Less;
            }
            if lhs_value.compare_greater_than(&rhs_value) == CmpBool::CmpTrue {
                return Ordering::Greater;
            }
        }
        Ordering::Equal
    }
}

impl<const KEY_SIZE: usize> Comparator<GenericKey<KEY_SIZE>> for GenericComparator {
    fn compare(&self, lhs: &GenericKey<KEY_SIZE>, rhs: &GenericKey<KEY_SIZE>) -> Ordering {
        self.compare(lhs, rhs)
    }
}
