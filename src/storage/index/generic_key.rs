use std::fmt;
use std::mem;

use crate::catalogue::schema::Schema;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;

/// A generic key used for indexing with opaque data.
#[derive(Clone)]
pub struct GenericKey<const KEY_SIZE: usize> {
    data: [u8; KEY_SIZE],
}

// impl<const KEY_SIZE: usize> GenericKey<KEY_SIZE> {
//     /// Initializes a new GenericKey.
//     pub fn new() -> Self {
//         Self {
//             data: [0; KEY_SIZE],
//         }
//     }
//
//     /// Sets the key data from a tuple.
//     ///
//     /// # Parameters
//     /// - `tuple`: The tuple to set the key from.
//     pub fn set_from_key(&mut self, tuple: &Tuple) {
//         self.data.fill(0);
//         let tuple_data = tuple.get_data();
//         let len = tuple_data.len().min(KEY_SIZE);
//         self.data[..len].copy_from_slice(&tuple_data[..len]);
//     }
//
//     /// Sets the key data from an integer.
//     ///
//     /// NOTE: For test purpose only.
//     ///
//     /// # Parameters
//     /// - `key`: The integer to set the key from.
//     pub fn set_from_integer(&mut self, key: i64) {
//         self.data.fill(0);
//         let key_bytes = key.to_le_bytes();
//         self.data[..mem::size_of::<i64>()].copy_from_slice(&key_bytes);
//     }
//
//     /// Converts the key to a value.
//     ///
//     /// # Parameters
//     /// - `schema`: The schema of the key.
//     /// - `column_idx`: The index of the column in the schema.
//     ///
//     /// # Returns
//     /// The value of the key.
//     pub fn to_value(&self, schema: &Schema, column_idx: usize) -> Value {
//         let col = schema.get_column(column_idx);
//         let column_type = col.expect("REASON").get_type();
//         let data_ptr = if col.is_inlined() {
//             &self.data[col.expect("REASON").get_offset()..]
//         } else {
//             let offset = i32::from_le_bytes(self.data[col.expect("REASON").get_offset()..col.expect("REASON").get_offset() + 4].try_into().unwrap());
//             &self.data[offset as usize..]
//         };
//         Value::deserialize_from(data_ptr, column_type)
//     }
//
//     /// Converts the key to a string.
//     ///
//     /// NOTE: For test purpose only.
//     ///
//     /// # Returns
//     /// The string representation of the key.
//     pub fn to_string(&self) -> i64 {
//         i64::from_le_bytes(self.data[..mem::size_of::<i64>()].try_into().unwrap())
//     }
// }
//
// impl<const KEY_SIZE: usize> fmt::Debug for GenericKey<KEY_SIZE> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{}", self.to_string())
//     }
// }
//
// impl<const KEY_SIZE: usize> fmt::Display for GenericKey<KEY_SIZE> {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         write!(f, "{}", self.to_string())
//     }
// }
//
// /// A comparator for generic keys.
// pub struct GenericComparator<'a, const KEY_SIZE: usize> {
//     key_schema: &'a Schema,
// }
//
// impl<'a, const KEY_SIZE: usize> GenericComparator<'a, KEY_SIZE> {
//     /// Creates a new GenericComparator.
//     ///
//     /// # Parameters
//     /// - `key_schema`: The schema of the keys.
//     pub fn new(key_schema: &'a Schema) -> Self {
//         Self { key_schema }
//     }
// }
