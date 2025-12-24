//! # Record
//!
//! This module provides the `Record` type, which combines a `Tuple` with its
//! storage location (RID) and optional schema metadata. Records are the primary
//! unit returned by table scans and query execution, providing both data and
//! context for higher-level processing.
//!
//! ## Architecture
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │                              Record                                     │
//!   │                                                                         │
//!   │  ┌─────────────────────────────────────────────────────────────────┐    │
//!   │  │                          Tuple                                  │    │
//!   │  │  ┌─────────┬─────────┬─────────┬─────────┐                      │    │
//!   │  │  │ Value 0 │ Value 1 │ Value 2 │   ...   │  (column values)     │    │
//!   │  │  └─────────┴─────────┴─────────┴─────────┘                      │    │
//!   │  └─────────────────────────────────────────────────────────────────┘    │
//!   │                                                                         │
//!   │  ┌───────────────────┐    ┌────────────────────────────────────────┐    │
//!   │  │        RID        │    │         Schema (optional)              │    │
//!   │  │  page_id: 5       │    │  Arc<Schema> for column names          │    │
//!   │  │  slot_num: 3      │    │  (not serialized, for formatting)      │    │
//!   │  └───────────────────┘    └────────────────────────────────────────┘    │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Record vs Tuple
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────┐
//!   │                        Comparison                                    │
//!   ├───────────────────┬──────────────────────────────────────────────────┤
//!   │     Component     │  Tuple              │  Record                    │
//!   ├───────────────────┼─────────────────────┼────────────────────────────┤
//!   │  Column values    │  ✓ (serialized)     │  ✓ (via inner Tuple)       │
//!   │  RID              │  ✓ (embedded)       │  ✓ (explicit field)        │
//!   │  Schema metadata  │  ✗                  │  ✓ (optional Arc<Schema>)  │
//!   │  Display/Debug    │  Synthetic names    │  Real column names         │
//!   │  Use case         │  Storage layer      │  Query results             │
//!   └───────────────────┴─────────────────────┴────────────────────────────┘
//! ```
//!
//! ## Serialization
//!
//! ```text
//!   On-Disk Format (via bincode)
//!   ─────────────────────────────────────────────────────────────────────────
//!
//!   What IS serialized:
//!   ┌─────────┬─────────┬─────────┬─────────┐
//!   │ Value 0 │ Value 1 │ Value 2 │   ...   │  ← Tuple data only
//!   └─────────┴─────────┴─────────┴─────────┘
//!
//!   What is NOT serialized:
//!   - RID (provided by page/slot context when loading)
//!   - Schema (attached at runtime by caller)
//!
//!   This design avoids redundancy: the page and slot already encode
//!   the record's location, so serializing RID would be wasteful.
//! ```
//!
//! ## Key Operations
//!
//! | Method | Description |
//! |--------|-------------|
//! | `new(tuple, rid, schema)` | Create record with location and schema |
//! | `serialize_to(buf)` | Serialize to byte buffer |
//! | `deserialize_from(buf)` | Deserialize from bytes (RID = default) |
//! | `get_value(idx)` | Get column value by index |
//! | `get_rid()` / `set_rid()` | Access/modify record location |
//! | `get_length()` | Get serialized size in bytes |
//! | `set_schema()` / `get_schema()` | Attach/retrieve schema metadata |
//! | `format_with_schema()` | Format with column names |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::storage::table::record::Record;
//! use crate::storage::table::tuple::Tuple;
//! use crate::common::rid::RID;
//!
//! // Create a record from a tuple
//! let values = vec![Value::new(1), Value::new("Alice")];
//! let tuple = Tuple::new(&values, &schema, RID::default());
//! let rid = RID::new(5, 3);  // page 5, slot 3
//! let record = Record::new(tuple, rid, schema.clone());
//!
//! // Access values
//! let id = record.get_value(0);      // → Value(1)
//! let name = record.get_value(1);    // → Value("Alice")
//! let location = record.get_rid();   // → RID(5, 3)
//!
//! // Format for display
//! println!("{}", record);
//! // → "RID: page_id: 5 slot_num: 3, id: 1, name: Alice"
//!
//! // Serialize to disk
//! let mut buf = vec![0u8; 1024];
//! let len = record.serialize_to(&mut buf)?;
//!
//! // Deserialize (RID must be reattached)
//! let mut loaded = Record::deserialize_from(&buf[..len])?;
//! loaded.set_rid(rid);  // Restore RID from page context
//! loaded.set_schema(schema);  // Restore schema for formatting
//! ```
//!
//! ## Display and Debug
//!
//! ```text
//!   Formatting Behavior
//!   ─────────────────────────────────────────────────────────────────────────
//!
//!   With schema attached:
//!     Display: "RID: page_id: 5 slot_num: 3, id: 1, name: Alice"
//!     Debug:   "Record { rid: page_id: 5 slot_num: 3, tuple: ... }"
//!
//!   Without schema:
//!     Display: "RID: page_id: 5 slot_num: 3, col_0: 1, col_1: Alice
//!               [schema unavailable; synthetic column names]"
//!
//!   The schema is optional so Records can be created quickly without
//!   requiring schema lookup, but formatting is richer when available.
//! ```
//!
//! ## Thread Safety
//!
//! - `Record` is `Send` and `Sync` when its inner types are
//! - `Schema` is wrapped in `Arc` for cheap cloning and sharing
//! - Records are typically owned by a single thread (query executor)

use crate::catalog::schema::Schema;
use crate::common::config::storage_bincode_config;
use crate::common::exception::TupleError;
use crate::common::rid::RID;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt::{Debug, Display, Formatter};
use std::io;
use std::sync::Arc;

/// Represents a tuple that has been stored in the database with a record ID.
#[allow(dead_code)]
pub struct Record {
    tuple: Tuple,
    rid: RID,
    /// Optional real schema metadata for human-friendly formatting.
    ///
    /// This is not serialized to disk; callers should attach the table schema
    /// when constructing or loading records so Display/Debug can surface real
    /// column names instead of synthetic ones.
    schema: Option<Arc<Schema>>,
}

// Implement bincode's Encode trait for Record
impl bincode::Encode for Record {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        // Encode in the same format as `Tuple` (values only).
        // RID is provided by page/slot context; schema metadata is intentionally not serialized.
        self.tuple.encode(encoder)
    }
}

// Implement bincode's Decode trait for Record
impl<C> bincode::Decode<C> for Record {
    fn decode<D: bincode::de::Decoder<Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let tuple: Tuple = bincode::Decode::decode(decoder)?;
        Ok(Self {
            tuple,
            // RID is not encoded; caller must set it after load.
            rid: RID::default(),
            schema: None,
        })
    }
}

// Provide a generic BorrowDecode implementation so Record works with bincode decode derives
impl<'de, C> bincode::BorrowDecode<'de, C> for Record {
    fn borrow_decode<D>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: bincode::de::BorrowDecoder<'de, Context = C>,
    {
        let tuple: Tuple = bincode::BorrowDecode::borrow_decode(decoder)?;
        Ok(Self {
            tuple,
            rid: RID::default(),
            schema: None,
        })
    }
}

impl PartialEq for Record {
    fn eq(&self, other: &Self) -> bool {
        // First compare RIDs for efficiency
        if self.rid != other.rid {
            return false;
        }

        // Then compare tuples
        self.tuple == other.tuple
    }
}

// Implement Eq for Record
impl Eq for Record {}

impl Record {
    /// Creates a new `Record` instance from a tuple and RID.
    ///
    /// Note: `Record` maintains the invariant that its `rid` matches the inner `Tuple` RID.
    pub fn new(mut tuple: Tuple, rid: RID, schema: Arc<Schema>) -> Self {
        tuple.set_rid(rid);
        Self {
            tuple,
            rid,
            schema: Some(schema),
        }
    }

    /// Attaches schema metadata to this record so formatting can use real column names.
    pub fn set_schema(&mut self, schema: Arc<Schema>) {
        self.schema = Some(schema);
    }

    /// Returns the attached schema, if present.
    pub fn get_schema(&self) -> Option<&Schema> {
        self.schema.as_deref()
    }

    /// Serializes the record into the given storage buffer using bincode 2.0.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if serialization fails or if the buffer is too small.
    pub fn serialize_to(&self, storage: &mut [u8]) -> Result<usize, TupleError> {
        // RID is not serialized; caller should set it from the surrounding page/slot.
        bincode::encode_into_slice(self, storage, storage_bincode_config()).map_err(|e| match e {
            // bincode 2.x uses this variant when the destination slice is too small.
            bincode::error::EncodeError::UnexpectedEnd => TupleError::BufferTooSmall,
            other => TupleError::SerializationError(other.to_string()),
        })
    }

    /// Deserializes a record from the given storage buffer using bincode 2.0.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if deserialization fails or if the buffer
    /// contains trailing bytes (must match the encoded length exactly).
    pub fn deserialize_from(storage: &[u8]) -> Result<Self, TupleError> {
        let (record, bytes_read): (Self, usize) =
            bincode::decode_from_slice(storage, storage_bincode_config())
            .map_err(|e| TupleError::DeserializationError(e.to_string()))?;
        if bytes_read != storage.len() {
            return Err(TupleError::DeserializationError(format!(
                "unexpected trailing bytes: consumed {bytes_read} of {}",
                storage.len()
            )));
        }
        Ok(record)
    }

    /// Returns the RID of the record.
    pub fn get_rid(&self) -> RID {
        self.rid
    }

    /// Sets the RID of the record.
    pub fn set_rid(&mut self, rid: RID) {
        self.rid = rid;
        self.tuple.set_rid(rid);
    }

    /// Returns the length of the serialized record using bincode 2.0.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if serialization fails.
    pub fn get_length(&self) -> Result<usize, TupleError> {
        #[derive(Default)]
        struct CountingWriter {
            len: usize,
        }

        impl io::Write for CountingWriter {
            fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
                self.len = self
                    .len
                    .checked_add(buf.len())
                    .ok_or_else(|| io::Error::new(io::ErrorKind::Other, "length overflow"))?;
                Ok(buf.len())
            }
            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let mut writer = CountingWriter::default();
        bincode::encode_into_std_write(self, &mut writer, storage_bincode_config())
            .map_err(|e| TupleError::SerializationError(e.to_string()))?;
        Ok(writer.len)
    }

    /// Convenience method to get a value directly from the record.
    pub fn get_value(&self, column_index: usize) -> Value {
        self.tuple.get_value(column_index)
    }

    /// Formats the record using the provided schema (column names).
    ///
    /// This intentionally avoids the name `to_string` to prevent confusion with the standard
    /// library's `ToString::to_string()` (which is implemented via `Display`).
    pub fn format_with_schema(&self, schema: &Schema) -> String {
        format!("RID: {}, {}", self.rid, self.tuple.format_with_schema(schema))
    }

    /// Formats the record using the provided schema, in a "detailed" form.
    ///
    /// Includes extra metadata useful for debugging. In particular, this prints both the record
    /// RID and the tuple RID so mismatches are obvious (they should normally be identical).
    pub fn format_with_schema_detailed(&self, schema: &Schema) -> String {
        let tuple_rid = self.tuple.get_rid();
        let rid_note = if tuple_rid == self.rid {
            String::new()
        } else {
            format!(" (tuple_rid differs: {})", tuple_rid)
        };

        format!(
            "Record {{ rid: {}{}, tuple: {} }}",
            self.rid,
            rid_note,
            self.tuple.format_with_schema_detailed(schema)
        )
    }

    /// Compatibility helper for older call sites that pass `Schema` by value.
    pub fn to_string_detailed_owned(&self, schema: Schema) -> String {
        self.format_with_schema_detailed(&schema)
    }

    /// Compatibility helper for older call sites that still use `to_string_detailed`.
    #[allow(clippy::wrong_self_convention)]
    pub fn to_string_detailed(&self, schema: &Schema) -> String {
        self.format_with_schema_detailed(schema)
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = self.schema.as_ref() {
            write!(f, "{}", self.format_with_schema(schema))
        } else {
            // Fall back to synthetic schema names but make the absence explicit.
            let schema = self.tuple.get_schema();
            write!(
                f,
                "{} [schema unavailable; synthetic column names]",
                self.format_with_schema(&schema)
            )
        }
    }
}

impl Debug for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        if let Some(schema) = self.schema.as_ref() {
            f.write_str(&self.format_with_schema_detailed(schema))
        } else {
            let schema = self.tuple.get_schema();
            f.write_str(&format!(
                "{} [schema unavailable; synthetic column names]",
                self.format_with_schema_detailed(&schema)
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::types_db::type_id::TypeId;

    fn create_sample_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("is_student", TypeId::Boolean),
        ])
    }

    fn create_sample_tuple() -> (Tuple, Arc<Schema>) {
        let schema = Arc::new(create_sample_schema());
        let values = vec![
            Value::new(1),
            Value::new("Alice"),
            Value::new(30),
            Value::new(true),
        ];
        let rid = RID::new(0, 0);
        (Tuple::new(&values, &schema, rid), schema)
    }

    fn create_sample_record() -> (Record, Arc<Schema>) {
        let (tuple, schema) = create_sample_tuple();
        let rid = RID::new(0, 0);
        (Record::new(tuple, rid, schema.clone()), schema)
    }

    #[test]
    fn test_record_creation_and_access() {
        let (record, _) = create_sample_record();

        assert_eq!(record.get_value(0), Value::new(1));
        assert_eq!(record.get_value(1), Value::new("Alice"));
        assert_eq!(record.get_value(2), Value::new(30));
        assert_eq!(record.get_value(3), Value::new(true));
        assert_eq!(record.get_rid(), RID::new(0, 0));
        assert_eq!(record.tuple.get_rid(), record.get_rid());
    }

    #[test]
    fn test_record_to_string_detailed() {
        let (record, schema) = create_sample_record();
        let expected = "RID: page_id: 0 slot_num: 0, id: 1, name: Alice, age: 30, is_student: true";
        assert_eq!(record.format_with_schema(&schema), expected);
    }

    #[test]
    fn test_record_serialization_deserialization() -> Result<(), TupleError> {
        let (record, _schema) = create_sample_record();

        let mut storage = vec![0u8; 1000];
        let serialized_len = record.serialize_to(&mut storage)?;

        let deserialized = Record::deserialize_from(&storage[..serialized_len])?;
        // RID is not persisted; it defaults until caller sets it.
        assert_eq!(deserialized.get_rid(), RID::default());
        assert_eq!(deserialized.get_value(0), record.get_value(0));
        assert_eq!(deserialized.get_value(1), record.get_value(1));
        assert_eq!(deserialized.get_value(2), record.get_value(2));
        assert_eq!(deserialized.get_value(3), record.get_value(3));

        // Caller can reattach the RID from context.
        let mut deserialized = deserialized;
        deserialized.set_rid(record.get_rid());
        assert_eq!(deserialized.get_rid(), record.get_rid());

        Ok(())
    }

    #[test]
    fn test_record_set_rid() {
        let (mut record, _) = create_sample_record();
        let new_rid = RID::new(1, 1);
        record.set_rid(new_rid);
        assert_eq!(record.get_rid(), new_rid);
        assert_eq!(record.tuple.get_rid(), new_rid);
    }

    #[test]
    fn test_record_new_overwrites_tuple_rid() {
        let schema = Arc::new(create_sample_schema());
        let values = vec![Value::new(1), Value::new("Alice"), Value::new(30), Value::new(true)];

        let tuple_rid = RID::new(9, 9);
        let tuple = Tuple::new(&values, &schema, tuple_rid);

        let record_rid = RID::new(1, 2);
        let record = Record::new(tuple, record_rid, schema.clone());

        assert_eq!(record.get_rid(), record_rid);
        assert_eq!(record.tuple.get_rid(), record_rid);
    }

    #[test]
    fn test_record_len() -> Result<(), TupleError> {
        let (record, _) = create_sample_record();
        let len = record.get_length()?;
        assert!(len > 0, "Serialized length should be greater than 0");
        Ok(())
    }

    #[test]
    fn test_direct_bincode_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let (record, _) = create_sample_record();

        // Directly use bincode 2.0 API
        let serialized = bincode::encode_to_vec(&record, storage_bincode_config())?;

        // Directly use bincode 2.0 API for deserialization
        let (deserialized, _): (Record, usize) =
            bincode::decode_from_slice(&serialized, storage_bincode_config())?;

        // Verify the deserialized record matches the original
        assert_eq!(deserialized.get_rid(), RID::default());
        assert_eq!(deserialized.get_value(0), record.get_value(0));
        assert_eq!(deserialized.get_value(1), record.get_value(1));
        assert_eq!(deserialized.get_value(2), record.get_value(2));
        assert_eq!(deserialized.get_value(3), record.get_value(3));

        Ok(())
    }

    #[test]
    fn test_record_display_uses_real_schema() {
        let (record, _schema) = create_sample_record();
        let rendered = record.to_string();
        assert!(rendered.contains("id: 1"));
        assert!(rendered.contains("name: Alice"));
    }

    #[test]
    fn test_record_deserialize_rejects_trailing_bytes() {
        let (record, _schema) = create_sample_record();
        let mut storage = vec![0u8; 1000];
        let serialized_len = record.serialize_to(&mut storage).unwrap();

        let mut with_trailing = storage[..serialized_len].to_vec();
        with_trailing.extend_from_slice(&[0u8, 1u8, 2u8]);

        let err = Record::deserialize_from(&with_trailing).unwrap_err();
        assert!(matches!(err, TupleError::DeserializationError(_)));
    }
}
