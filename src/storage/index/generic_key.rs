use std::cmp::Ordering;
use std::error::Error;
use std::fmt::Display;
use std::mem::size_of;
use std::sync::Arc;

use bincode;
use serde::de::{SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::exception::{ComparisonError, KeyConversionError, TupleError};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::Value;

/// A generic key used for indexing with opaque data.
#[derive(Clone)]
pub struct GenericKey<const KEY_SIZE: usize> {
    data: [u8; KEY_SIZE],
}

impl<const KEY_SIZE: usize> GenericKey<KEY_SIZE> {
    /// Creates a new GenericKey with zeroed data.
    pub fn new() -> Self {
        Self { data: [0; KEY_SIZE] }
    }

    /// Sets the key data from a tuple.
    ///
    /// # Arguments
    ///
    /// * `tuple` - The tuple to set the key from.
    ///
    /// # Returns
    ///
    /// A Result indicating success or a TupleError if serialization fails.
    pub fn set_from_tuple(&mut self, tuple: &Tuple) -> Result<(), TupleError> {
        self.data.fill(0);
        let mut buffer = vec![0u8; KEY_SIZE];
        let serialized_len = tuple.serialize_to(&mut buffer)?;
        let len = serialized_len.min(KEY_SIZE);
        self.data[..len].copy_from_slice(&buffer[..len]);
        Ok(())
    }

    /// Sets the key data from an integer.
    ///
    /// # Arguments
    ///
    /// * `key` - The integer to set the key from.
    pub fn set_from_integer(&mut self, key: i64) {
        self.data.fill(0);
        let key_bytes = key.to_le_bytes();
        self.data[..size_of::<i64>()].copy_from_slice(&key_bytes);
    }

    /// Attempts to convert the key to a value using `bincode`.
    ///
    /// # Arguments
    ///
    /// * `schema` - The schema of the key.
    /// * `column_idx` - The index of the column in the schema.
    ///
    /// # Returns
    ///
    /// A Result containing a Value representation of the key, or an error if conversion fails.
    pub fn to_value(&self, schema: &Schema, column_idx: usize) -> Result<Value, KeyConversionError> {
        // Get the column from the schema
        let col = schema
            .get_column(column_idx)
            .ok_or_else(|| KeyConversionError::ColumnNotFound(format!("Index {} not found", column_idx)))?;

        // Determine the data pointer
        let data_ptr = if col.is_inlined() {
            &self.data[col.get_offset()..]
        } else {
            let offset_bytes = self.data.get(col.get_offset()..col.get_offset() + 4)
                .ok_or_else(|| KeyConversionError::OffsetConversionError("Failed to extract offset bytes".to_string()))?;

            let offset = i32::from_le_bytes(offset_bytes.try_into().map_err(|_| {
                KeyConversionError::OffsetConversionError("Failed to convert offset bytes to i32".to_string())
            })?);

            self.data.get(offset as usize..).ok_or_else(|| {
                KeyConversionError::OffsetConversionError(format!("Invalid offset: {}", offset))
            })?
        };

        // Attempt to deserialize the value using bincode
        bincode::deserialize(data_ptr).map_err(|e| KeyConversionError::DeserializationError(e.to_string()))
    }

    /// Creates a GenericKey from a byte slice.
    ///
    /// # Arguments
    ///
    /// * `bytes` - The byte slice to create the key from.
    ///
    /// # Returns
    ///
    /// A new GenericKey instance.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut key = Self::new();
        key.set_from_key_slice(bytes);
        key
    }

    /// Sets the key data from a byte slice.
    ///
    /// # Arguments
    ///
    /// * `key` - The byte slice to set the key from.
    pub fn set_from_key_slice(&mut self, key: &[u8]) {
        self.data.fill(0);
        let len = key.len().min(KEY_SIZE);
        self.data[..len].copy_from_slice(&key[..len]);
    }

    /// Returns a reference to the key's byte data.
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }
}

/// Function object that compares GenericKeys based on a schema.
#[derive(Debug, Clone)]
pub struct Comparator {
    key_schema: Arc<Schema>,
}

impl Comparator {
    /// Creates a new GenericComparator.
    ///
    /// # Arguments
    ///
    /// * `key_schema` - The schema used for comparison.
    pub fn new(key_schema: Arc<Schema>) -> Self {
        Self { key_schema }
    }

    /// Compares two GenericKeys.
    ///
    /// # Arguments
    ///
    /// * `lhs` - The left-hand side key.
    /// * `rhs` - The right-hand side key.
    ///
    /// # Returns
    ///
    /// A Result containing either an Ordering representing the comparison result or a ComparisonError.
    pub fn compare<const KEY_SIZE: usize>(
        &self,
        lhs: &GenericKey<KEY_SIZE>,
        rhs: &GenericKey<KEY_SIZE>,
    ) -> Result<Ordering, ComparisonError> {
        for i in 0..self.key_schema.get_column_count() {
            // Try to retrieve values from both sides
            let (lhs_value, rhs_value) = match (
                lhs.to_value(&self.key_schema, i as usize),
                rhs.to_value(&self.key_schema, i as usize),
            ) {
                (Ok(lhs_value), Ok(rhs_value)) => (lhs_value, rhs_value),
                (Err(e), _) | (_, Err(e)) => {
                    return Err(ComparisonError::ValueRetrievalError(format!(
                        "Failed to retrieve value: {}",
                        e
                    )));
                }
            };

            // Compare the two values
            if lhs_value.compare_less_than(&rhs_value) == CmpBool::CmpTrue {
                return Ok(Ordering::Less);
            }
            if lhs_value.compare_greater_than(&rhs_value) == CmpBool::CmpTrue {
                return Ok(Ordering::Greater);
            }
        }
        Ok(Ordering::Equal)
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generic_key_new() {
        let key: GenericKey<16> = GenericKey::new();
        assert_eq!(key.data, [0; 16]);
    }

    #[test]
    fn generic_key_set_from_integer() {
        let mut key: GenericKey<16> = GenericKey::new();
        key.set_from_integer(42);
        assert_eq!(key.data[..8], 42i64.to_le_bytes());
        assert_eq!(key.data[8..], [0; 8]);
    }

    #[test]
    fn generic_key_from_bytes() {
        let bytes = [1, 2, 3, 4, 5, 6, 7, 8];
        let key: GenericKey<16> = GenericKey::from_bytes(&bytes);
        assert_eq!(key.data[..8], bytes);
        assert_eq!(key.data[8..], [0; 8]);
    }

    #[test]
    fn generic_key_to_value() {
        let schema = Arc::new(Schema::new(vec![Column::new("id", TypeId::Integer)]));
        let mut key: GenericKey<16> = GenericKey::new();
        key.set_from_integer(42);

        let value = key.to_value(&schema, 0);
        assert_eq!(value.unwrap(), Value::new(42i32));
    }

    #[test]
    fn generic_key_to_value_error() {
        let schema = Arc::new(Schema::new(vec![Column::new("id", TypeId::Integer)]));
        let key: GenericKey<16> = GenericKey::new(); // Empty key

        assert!(key.to_value(&schema, 0).is_err());
    }

    #[test]
    fn generic_comparator() {
        let schema = Arc::new(Schema::new(vec![Column::new("id", TypeId::Integer)]));
        let comparator = Comparator::new(schema);

        let mut key1: GenericKey<16> = GenericKey::new();
        let mut key2: GenericKey<16> = GenericKey::new();
        let mut key3: GenericKey<16> = GenericKey::new();

        key1.set_from_integer(1);
        key2.set_from_integer(1);
        key3.set_from_integer(100);

        assert_eq!(comparator.compare(&key1, &key2).unwrap(), Ordering::Equal);
        assert_eq!(comparator.compare(&key1, &key3).unwrap(), Ordering::Less);
        assert_eq!(comparator.compare(&key3, &key2).unwrap(), Ordering::Greater);
    }
}