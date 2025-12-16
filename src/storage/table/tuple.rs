use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{storage_bincode_config, Timestamp, TxnId};
use crate::common::exception::TupleError;
use crate::common::rid::RID;
use crate::types_db::value::Value;
use std::fmt::{Display, Formatter};
use std::io;

/// Metadata associated with a tuple.
#[derive(Debug, PartialEq, Copy, Clone, bincode::Encode, bincode::Decode)]
pub struct TupleMeta {
    creator_txn_id: TxnId,
    /// Commit timestamp for this tuple version.
    ///
    /// `None` means the version is uncommitted (in-progress) and must not be treated as having
    /// any real commit timestamp.
    commit_timestamp: Option<Timestamp>,
    deleted: bool,
    /// Index into the owning transaction's undo log chain.
    ///
    /// Stored as a `u64` (not `usize`) to keep representation stable across architectures.
    ///
    /// `None` means this tuple version has no undo-log link (i.e., the version chain ends here).
    undo_log_idx: Option<u64>,
}

impl TupleMeta {
    /// Creates a new `TupleMeta` instance.
    pub fn new(txn_id: TxnId) -> Self {
        Self {
            creator_txn_id: txn_id,
            commit_timestamp: None,
            deleted: false,
            undo_log_idx: None,
        }
    }

    /// Creates a new `TupleMeta` instance with specified deleted status.
    pub fn new_with_delete(txn_id: TxnId, deleted: bool) -> Self {
        Self {
            creator_txn_id: txn_id,
            commit_timestamp: None,
            deleted,
            undo_log_idx: None,
        }
    }

    /// Returns the transaction ID that created/modified the tuple.
    pub fn get_creator_txn_id(&self) -> TxnId {
        self.creator_txn_id
    }

    /// Returns the commit timestamp when the tuple became visible (if committed).
    pub fn get_commit_timestamp(&self) -> Option<Timestamp> {
        self.commit_timestamp
    }

    /// Sets the commit timestamp when the tuple became visible.
    pub fn set_commit_timestamp(&mut self, ts: Timestamp) {
        self.commit_timestamp = Some(ts);
    }

    /// Sets the commit timestamp (or clears it).
    ///
    /// `None` means the version is uncommitted/in-progress.
    pub fn set_commit_timestamp_opt(&mut self, ts: Option<Timestamp>) {
        self.commit_timestamp = ts;
    }

    /// Returns whether the tuple is committed.
    pub fn is_committed(&self) -> bool {
        self.commit_timestamp.is_some()
    }

    /// Returns whether the tuple is marked as deleted.
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    pub fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }

    /// Checks if this tuple version is visible to the given transaction.
    ///
    /// `read_ts` is the *snapshot/read timestamp* of the transaction performing the read
    /// (i.e., the timestamp the transaction should observe). This must **not** be the global
    /// low-watermark used for GC, since that would incorrectly hide versions from newer readers
    /// when an older reader is still active.
    ///
    /// Note: deleted versions act as tombstones. Readers that need “previous visible” semantics
    /// must traverse the version chain (undo logs) to locate an older visible version.
    pub fn is_visible_to(&self, txn_id: TxnId, read_ts: Timestamp) -> bool {
        // If this tuple version was created by the current transaction, it's visible as long as
        // it isn't deleted by the same transaction.
        if self.creator_txn_id == txn_id {
            return !self.deleted;
        }

        // If the tuple is deleted, it's not visible
        if self.deleted {
            return false;
        }

        // If the tuple is not committed, it's not visible
        if !self.is_committed() {
            return false;
        }

        // The tuple is visible if its commit timestamp is <= the reader's snapshot.
        self.commit_timestamp
            .is_some_and(|commit_ts| commit_ts <= read_ts)
    }

    /// Gets the undo log index for this tuple version
    pub fn try_get_undo_log_idx(&self) -> Result<usize, TupleError> {
        self.try_get_undo_log_idx_opt()?
            .ok_or(TupleError::UndoLogIndexMissing)
    }

    /// Gets the undo log index for this tuple version, if present.
    ///
    /// Returns `Ok(None)` if this tuple version has no undo-log link.
    pub fn try_get_undo_log_idx_opt(&self) -> Result<Option<usize>, TupleError> {
        let Some(raw) = self.undo_log_idx else {
            return Ok(None);
        };
        usize::try_from(raw)
            .map(Some)
            .map_err(|_| TupleError::UndoLogIndexOverflow(raw))
    }

    /// Sets the undo log index for this tuple version
    pub fn set_undo_log_idx(&mut self, idx: usize) -> Result<(), TupleError> {
        self.undo_log_idx =
            Some(u64::try_from(idx).map_err(|_| TupleError::UndoLogIndexU64Overflow(idx))?);
        Ok(())
    }

    /// Clears the undo log index for this tuple version (meaning "no undo-log link").
    pub fn clear_undo_log_idx(&mut self) {
        self.undo_log_idx = None;
    }

    /// Returns whether this tuple version has an undo-log link.
    pub fn has_undo_log_idx(&self) -> bool {
        self.undo_log_idx.is_some()
    }

    /// Gets the raw, fixed-width undo log index stored in the tuple metadata.
    ///
    /// This is intended for debugging/diagnostics and for on-disk format stability. Most callers
    /// should use `try_get_undo_log_idx()` (since in-memory undo logs are indexed by `usize`).
    pub fn get_undo_log_idx_raw_opt(&self) -> Option<u64> {
        self.undo_log_idx
    }

    /// Sets the creator transaction ID for this tuple
    pub fn set_creator_txn_id(&mut self, txn_id: TxnId) {
        self.creator_txn_id = txn_id;
    }
}

/// Represents a tuple in the database.
#[derive(Debug, Clone)]
pub struct Tuple {
    /// Tuple values. `Tuple` has value semantics: cloning a tuple clones its values.
    values: Vec<Value>,
    rid: RID,
}

// Implement bincode's Encode trait for Tuple
impl bincode::Encode for Tuple {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        self.values.encode(encoder)?;
        self.rid.encode(encoder)
    }
}

// Implement bincode's Decode trait for Tuple
impl<C> bincode::Decode<C> for Tuple {
    fn decode<D: bincode::de::Decoder<Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let values: Vec<Value> = bincode::Decode::decode(decoder)?;
        let rid: RID = bincode::Decode::decode(decoder)?;
        Ok(Self {
            values,
            rid,
        })
    }
}

// Provide a generic BorrowDecode implementation so Tuple works with bincode decode derives
impl<'de, C> bincode::BorrowDecode<'de, C> for Tuple {
    fn borrow_decode<D>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: bincode::de::BorrowDecoder<'de, Context = C>,
    {
        let values: Vec<Value> = bincode::BorrowDecode::borrow_decode(decoder)?;
        let rid: RID = bincode::BorrowDecode::borrow_decode(decoder)?;
        Ok(Self {
            values,
            rid,
        })
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        // First compare RIDs for efficiency (avoid value comparisons if RIDs differ).
        if self.rid != other.rid {
            return false;
        }

        self.values == other.values
    }
}

// Implement Eq as well since tuples can be equal if they have the same RID and values
impl Eq for Tuple {}

impl Tuple {
    /// Creates a new `Tuple` instance.
    ///
    /// # Panics
    ///
    /// Panics if the number of values doesn't match the schema's column count.
    pub fn new(values: &[Value], schema: &Schema, rid: RID) -> Self {
        Self::try_new(values, schema, rid).unwrap_or_else(|e| {
            panic!("Values length does not match schema column count: {e}")
        })
    }

    /// Fallible constructor that validates the value count against the schema.
    pub fn try_new(values: &[Value], schema: &Schema, rid: RID) -> Result<Self, TupleError> {
        let expected = schema.get_column_count() as usize;
        let actual = values.len();
        if actual != expected {
            return Err(TupleError::ValueCountMismatch { expected, actual });
        }
        Ok(Self {
            values: values.to_vec(),
            rid,
        })
    }

    /// Serializes the tuple into the given storage buffer.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if serialization fails or if the buffer is too small.
    pub fn serialize_to(&self, storage: &mut [u8]) -> Result<usize, TupleError> {
        bincode::encode_into_slice(self, storage, storage_bincode_config()).map_err(|e| match e {
            // bincode 2.x uses this variant when the destination slice is too small.
            bincode::error::EncodeError::UnexpectedEnd => TupleError::BufferTooSmall,
            other => TupleError::SerializationError(other.to_string()),
        })
    }

    /// Deserializes a tuple from the given storage buffer.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if deserialization fails.
    pub fn deserialize_from(storage: &[u8]) -> Result<Self, TupleError> {
        bincode::decode_from_slice(storage, storage_bincode_config())
            .map_err(|e| TupleError::DeserializationError(e.to_string()))
            .map(|(tuple, _)| tuple)
    }

    /// Returns the RID of the tuple.
    pub fn get_rid(&self) -> RID {
        self.rid
    }

    /// Sets the RID of the tuple.
    pub fn set_rid(&mut self, rid: RID) {
        self.rid = rid;
    }

    /// Returns the length of the serialized tuple.
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

    /// Returns a reference to the value at the given column index.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds.
    pub fn get_value(&self, column_index: usize) -> Value {
        self.values[column_index].clone()
    }

    /// Borrow the value at the given column index.
    ///
    /// This avoids cloning and is preferred for hot paths like scans/predicates.
    pub fn get_value_ref(&self, column_index: usize) -> &Value {
        &self.values[column_index]
    }

    /// Replaces this tuple's values **without** validating against any schema.
    ///
    /// This can temporarily put the tuple in a state that doesn't match a particular `Schema`.
    /// Prefer `set_values_checked()` when you have the intended schema available.
    pub fn set_values(&mut self, values: Vec<Value>) {
        self.values = values;
    }

    /// Replaces this tuple's values, validating the value count against the given schema.
    pub fn set_values_checked(
        &mut self,
        values: Vec<Value>,
        schema: &Schema,
    ) -> Result<(), TupleError> {
        let expected = schema.get_column_count() as usize;
        let actual = values.len();
        if actual != expected {
            return Err(TupleError::ValueCountMismatch { expected, actual });
        }
        self.values = values;
        Ok(())
    }

    pub fn get_values(&self) -> Vec<Value> {
        self.values.clone()
    }

    /// Borrow all values in the tuple without cloning.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn get_values_mut(&mut self) -> &mut Vec<Value> {
        &mut self.values
    }

    /// Returns a reference to the schema of this tuple.
    ///
    /// This method constructs a schema based on the types of values in the tuple.
    pub fn get_schema(&self) -> Schema {
        let columns: Vec<Column> = self
            .values
            .iter()
            .enumerate()
            .map(|(i, value)| Column::new(&format!("col_{}", i), value.get_type_id()))
            .collect();

        Schema::new(columns)
    }

    /// Creates a new tuple containing only the key attributes.
    pub fn keys_from_tuple(&self, key_attrs: &[usize]) -> Vec<Value> {
        key_attrs
            .iter()
            .map(|&attr| self.values[attr].clone())
            .collect()
    }

    /// Creates a new tuple containing only the key attributes, returning an error if any key
    /// attribute index is out of bounds.
    pub fn keys_from_tuple_checked(&self, key_attrs: &[usize]) -> Result<Vec<Value>, TupleError> {
        let column_count = self.values.len();
        let mut keys = Vec::with_capacity(key_attrs.len());
        for &attr in key_attrs {
            if attr >= column_count {
                return Err(TupleError::KeyAttrOutOfBounds { attr, column_count });
            }
            keys.push(self.values[attr].clone());
        }
        Ok(keys)
    }

    /// Returns a string representation of the tuple.
    pub fn to_string(&self, schema: &Schema) -> String {
        self.to_string_with_schema(schema)
    }

    /// Returns a detailed string representation of the tuple.
    pub fn to_string_detailed(&self, schema: &Schema) -> String {
        // Currently identical to `to_string`; keep this entrypoint for compatibility.
        self.to_string_with_schema(schema)
    }

    /// Compatibility helper for older call sites that pass `Schema` by value.
    pub fn to_string_owned(&self, schema: Schema) -> String {
        self.to_string(&schema)
    }

    /// Compatibility helper for older call sites that pass `Schema` by value.
    pub fn to_string_detailed_owned(&self, schema: Schema) -> String {
        self.to_string_detailed(&schema)
    }

    /// Like `to_string`, but avoids moving/cloning the schema.
    pub fn to_string_with_schema(&self, schema: &Schema) -> String {
        self.values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let col_name = schema
                    .get_column(i)
                    .map(|col| col.get_name().to_string())
                    .unwrap_or_else(|| format!("col_{}", i));
                format!("{}: {}", col_name, value)
            })
            .collect::<Vec<String>>()
            .join(", ")
    }

    /// Combines this tuple with another tuple by appending the other tuple's values
    pub fn combine(&self, other: &Tuple) -> Self {
        let mut combined_values = Vec::with_capacity(self.values.len() + other.values.len());
        combined_values.extend(self.values.iter().cloned());
        combined_values.extend(other.values.iter().cloned());

        Self {
            values: combined_values,
            // Combined tuples are derived values (e.g., joins) and do not correspond to a single
            // base-table record. Use an invalid RID sentinel to avoid accidental misuse.
            rid: RID::default(),
        }
    }

    /// Gets the number of columns in this tuple
    pub fn get_column_count(&self) -> usize {
        self.values.len()
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let schema = self.get_schema();
        write!(f, "{}", self.to_string_detailed(&schema))
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

    fn create_sample_tuple() -> (Tuple, Schema) {
        let schema = create_sample_schema();
        let values = vec![
            Value::new(1),
            Value::new("Alice"),
            Value::new(30),
            Value::new(true),
        ];
        let rid = RID::new(0, 0);
        (Tuple::new(&values, &schema, rid), schema)
    }

    #[test]
    fn test_tuple_creation_and_access() {
        let (tuple, _) = create_sample_tuple();

        assert_eq!(tuple.get_value(0), Value::new(1));
        assert_eq!(tuple.get_value(1), Value::new("Alice"));
        assert_eq!(tuple.get_value(2), Value::new(30));
        assert_eq!(tuple.get_value(3), Value::new(true));
        assert_eq!(tuple.get_rid(), RID::new(0, 0));
    }

    #[test]
    fn test_tuple_clone_is_independent() {
        let (tuple, schema) = create_sample_tuple();
        let mut cloned = tuple.clone();

        // Mutate the clone in-place.
        cloned.get_values_mut()[0] = Value::new(999);

        // Original must remain unchanged (value semantics).
        assert_eq!(tuple.get_value(0), Value::new(1));
        assert_eq!(cloned.get_value(0), Value::new(999));

        // Both should still serialize/format correctly.
        assert_eq!(
            tuple.to_string(&schema),
            "id: 1, name: Alice, age: 30, is_student: true"
        );
        assert_eq!(
            cloned.to_string(&schema),
            "id: 999, name: Alice, age: 30, is_student: true"
        );
    }

    #[test]
    fn test_tuple_to_string_detailed() {
        let (tuple, _) = create_sample_tuple();
        let schema = create_sample_schema();
        let expected = "id: 1, name: Alice, age: 30, is_student: true";
        assert_eq!(tuple.to_string_detailed(&schema), expected);
    }

    #[test]
    fn test_tuple_serialization_deserialization() -> Result<(), TupleError> {
        let (tuple, _schema) = create_sample_tuple();

        let mut storage = vec![0u8; 1000];
        let serialized_len = tuple.serialize_to(&mut storage)?;

        let deserialized = Tuple::deserialize_from(&storage[..serialized_len])?;
        assert_eq!(deserialized.get_rid(), tuple.get_rid());
        assert_eq!(deserialized.get_value(0), tuple.get_value(0));
        assert_eq!(deserialized.get_value(1), tuple.get_value(1));
        assert_eq!(deserialized.get_value(2), tuple.get_value(2));
        assert_eq!(deserialized.get_value(3), tuple.get_value(3));

        Ok(())
    }

    #[test]
    fn test_tuple_keys_from_tuple() {
        let (tuple, _schema) = create_sample_tuple();

        let key_attrs = vec![0, 2];
        let keys = tuple.keys_from_tuple_checked(&key_attrs).unwrap();

        assert_eq!(keys[0], Value::new(1));
        assert_eq!(keys[1], Value::new(30));
    }

    #[test]
    fn test_tuple_keys_from_tuple_checked_out_of_bounds() {
        let (tuple, _schema) = create_sample_tuple();
        let err = tuple.keys_from_tuple_checked(&[999]).unwrap_err();
        match err {
            TupleError::KeyAttrOutOfBounds { attr, column_count } => {
                assert_eq!(attr, 999);
                assert_eq!(column_count, tuple.get_column_count());
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn test_tuple_to_string() {
        let (tuple, _) = create_sample_tuple();
        let schema = create_sample_schema();
        let expected = "id: 1, name: Alice, age: 30, is_student: true";
        assert_eq!(tuple.to_string(&schema), expected);
    }

    #[test]
    #[should_panic(expected = "Values length does not match schema column count")]
    fn test_tuple_creation_with_mismatched_schema() {
        let schema = create_sample_schema();
        let values = vec![Value::new(1), Value::new("Alice")]; // Missing two values
        let rid = RID::new(0, 0);
        Tuple::new(&values, &schema, rid);
    }

    #[test]
    fn test_tuple_set_rid() {
        let (mut tuple, _) = create_sample_tuple();
        let new_rid = RID::new(1, 1);
        tuple.set_rid(new_rid);
        assert_eq!(tuple.get_rid(), new_rid);
    }

    #[test]
    fn test_tuple_len() -> Result<(), TupleError> {
        let (tuple, _) = create_sample_tuple();
        let len = tuple.get_length()?;
        assert!(len > 0, "Serialized length should be greater than 0");
        Ok(())
    }

    #[test]
    fn test_direct_bincode_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let (tuple, _) = create_sample_tuple();
        let config = storage_bincode_config();

        // Directly use bincode with the Encode trait
        let serialized = bincode::encode_to_vec(&tuple, config)?;

        // Directly use bincode with the Decode trait
        let (deserialized, _): (Tuple, usize) = bincode::decode_from_slice(&serialized, config)?;

        // Verify the deserialized tuple matches the original
        assert_eq!(deserialized.get_rid(), tuple.get_rid());
        assert_eq!(deserialized.get_value(0), tuple.get_value(0));
        assert_eq!(deserialized.get_value(1), tuple.get_value(1));
        assert_eq!(deserialized.get_value(2), tuple.get_value(2));
        assert_eq!(deserialized.get_value(3), tuple.get_value(3));

        Ok(())
    }

    #[test]
    fn test_tuple_meta() {
        let mut meta = TupleMeta::new(1);
        assert_eq!(meta.get_creator_txn_id(), 1);
        assert!(!meta.is_committed());
        assert!(!meta.is_deleted());
        assert!(!meta.has_undo_log_idx());

        meta.set_deleted(true);
        assert!(meta.is_deleted());
    }

    #[test]
    fn test_tuple_meta_visibility_rules() {
        // Creator transaction sees its own uncommitted version (unless deleted).
        let meta = TupleMeta::new(1);
        assert!(meta.is_visible_to(1, 1));

        let meta = TupleMeta::new_with_delete(1, true);
        assert!(!meta.is_visible_to(1, 1));

        // Other transactions do not see uncommitted versions.
        let mut meta = TupleMeta::new(1);
        assert!(!meta.is_visible_to(2, 100));

        // Committed versions are visible based on the reader's snapshot timestamp.
        meta.set_commit_timestamp(5);
        assert!(meta.is_visible_to(2, 5));
        assert!(meta.is_visible_to(2, 10));
        assert!(!meta.is_visible_to(2, 4));

        // Deletes are never visible to other transactions.
        meta.set_deleted(true);
        assert!(!meta.is_visible_to(2, 10));
    }

    #[test]
    fn test_tuple_meta_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let meta = TupleMeta::new(1234567890);
        let config = storage_bincode_config();
        let serialized = bincode::encode_to_vec(meta, config)?;
        let (deserialized, _): (TupleMeta, usize) =
            bincode::decode_from_slice(&serialized, config)?;

        assert_eq!(meta.get_creator_txn_id(), deserialized.get_creator_txn_id());
        assert_eq!(meta.is_committed(), deserialized.is_committed());
        assert_eq!(meta.is_deleted(), deserialized.is_deleted());

        Ok(())
    }

    #[test]
    fn test_tuple_with_different_value_types() {
        let schema = Schema::new(vec![
            Column::new("int", TypeId::Integer),
            Column::new("float", TypeId::Decimal),
            Column::new("string", TypeId::VarChar),
            Column::new("bool", TypeId::Boolean),
        ]);
        let values = vec![
            Value::new(42),
            Value::new(std::f64::consts::PI),
            Value::new("Hello"),
            Value::new(true),
        ];
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&values, &schema, rid);

        assert_eq!(tuple.get_value(0), Value::new(42));
        assert_eq!(tuple.get_value(1), Value::new(std::f64::consts::PI));
        assert_eq!(tuple.get_value(2), Value::new("Hello"));
        assert_eq!(tuple.get_value(3), Value::new(true));
    }

    #[test]
    fn test_get_schema() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        let values = vec![Value::new(1), Value::new("Alice"), Value::new(30)];

        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&values, &schema, rid);

        let derived_schema = tuple.get_schema();

        // Check that the number of columns matches
        assert_eq!(derived_schema.get_column_count(), schema.get_column_count());

        // Check that the types match for each column
        for i in 0..schema.get_column_count() {
            assert_eq!(
                derived_schema.get_column(i as usize).unwrap().get_type(),
                schema.get_column(i as usize).unwrap().get_type(),
                "Type mismatch for column {}",
                i
            );
        }
    }
}
