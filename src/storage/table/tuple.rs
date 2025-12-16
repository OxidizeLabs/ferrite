use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{Timestamp, TxnId};
use crate::common::exception::TupleError;
use crate::common::rid::RID;
use crate::concurrency::watermark::Watermark;
use crate::types_db::value::Value;
use bincode::config;
use log;
use parking_lot::{RwLock, RwLockWriteGuard};
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Metadata associated with a tuple.
#[derive(Debug, PartialEq, Copy, Clone, bincode::Encode, bincode::Decode)]
pub struct TupleMeta {
    creator_txn_id: TxnId,
    commit_timestamp: Timestamp,
    deleted: bool,
    undo_log_idx: usize,
}

impl TupleMeta {
    /// Creates a new `TupleMeta` instance.
    pub fn new(txn_id: TxnId) -> Self {
        Self {
            creator_txn_id: txn_id,
            commit_timestamp: Timestamp::MAX, // Start with MAX to indicate uncommitted
            deleted: false,
            undo_log_idx: 0,
        }
    }

    /// Creates a new `TupleMeta` instance with specified deleted status.
    pub fn new_with_delete(txn_id: TxnId, deleted: bool) -> Self {
        Self {
            creator_txn_id: txn_id,
            commit_timestamp: Timestamp::MAX, // Start with MAX to indicate uncommitted
            deleted,
            undo_log_idx: 0,
        }
    }

    /// Returns the transaction ID that created/modified the tuple.
    pub fn get_creator_txn_id(&self) -> TxnId {
        self.creator_txn_id
    }

    /// Returns the commit timestamp when the tuple became visible.
    pub fn get_commit_timestamp(&self) -> Timestamp {
        self.commit_timestamp
    }

    /// Sets the commit timestamp when the tuple became visible.
    pub fn set_commit_timestamp(&mut self, ts: Timestamp) {
        self.commit_timestamp = ts;
    }

    /// Returns whether the tuple is committed.
    pub fn is_committed(&self) -> bool {
        let committed = self.commit_timestamp != Timestamp::MAX;
        log::debug!(
            "TupleMeta.is_committed(): commit_ts={}, is_committed={}",
            self.commit_timestamp,
            committed
        );
        committed
    }

    /// Returns whether the tuple is marked as deleted.
    pub fn is_deleted(&self) -> bool {
        self.deleted
    }

    pub fn set_deleted(&mut self, deleted: bool) {
        self.deleted = deleted;
    }

    /// Checks if this tuple is visible to the given transaction.
    pub fn is_visible_to(&self, txn_id: TxnId, watermark: &Watermark) -> bool {
        log::debug!(
            "TupleMeta.is_visible_to(): creator_txn={}, current_txn={}, commit_ts={}, deleted={}, watermark={}",
            self.creator_txn_id,
            txn_id,
            self.commit_timestamp,
            self.deleted,
            watermark.get_watermark()
        );

        // If this tuple was created by the current transaction, it's visible
        if self.creator_txn_id == txn_id {
            log::debug!("Tuple visible: created by current transaction");
            return true;
        }

        // If the tuple is deleted, it's not visible
        if self.deleted {
            log::debug!("Tuple not visible: deleted");
            return false;
        }

        // If the tuple is not committed, it's not visible
        if !self.is_committed() {
            log::debug!("Tuple not visible: not committed");
            return false;
        }

        // The tuple is visible if its commit timestamp is less than the watermark
        let visible = self.commit_timestamp <= watermark.get_watermark();
        log::debug!(
            "Tuple visibility based on watermark: commit_ts={} <= watermark={} = {}",
            self.commit_timestamp,
            watermark.get_watermark(),
            visible
        );
        visible
    }

    /// Gets the undo log index for this tuple version
    pub fn get_undo_log_idx(&self) -> usize {
        self.undo_log_idx
    }

    /// Sets the undo log index for this tuple version
    pub fn set_undo_log_idx(&mut self, idx: usize) {
        self.undo_log_idx = idx;
    }

    /// Sets the creator transaction ID for this tuple
    pub fn set_creator_txn_id(&mut self, txn_id: TxnId) {
        self.creator_txn_id = txn_id;
    }
}

/// Represents a tuple in the database.
#[derive(Debug, Clone)]
pub struct Tuple {
    values: Arc<RwLock<Vec<Value>>>,
    rid: RID,
}

// Implement bincode's Encode trait for Tuple
impl bincode::Encode for Tuple {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> Result<(), bincode::error::EncodeError> {
        let values = self.values.read().clone();
        (values, self.rid).encode(encoder)
    }
}

// Implement bincode's Decode trait for Tuple
impl<C> bincode::Decode<C> for Tuple {
    fn decode<D: bincode::de::Decoder<Context = C>>(
        decoder: &mut D,
    ) -> Result<Self, bincode::error::DecodeError> {
        let (values, rid): (Vec<Value>, RID) = bincode::Decode::decode(decoder)?;
        Ok(Self {
            values: Arc::new(RwLock::new(values)),
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
        let (values, rid): (Vec<Value>, RID) = bincode::BorrowDecode::borrow_decode(decoder)?;
        Ok(Self {
            values: Arc::new(RwLock::new(values)),
            rid,
        })
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        // First compare RIDs for efficiency (avoid lock acquisition if RIDs differ)
        if self.rid != other.rid {
            return false;
        }

        // Compare the contents of values by acquiring read locks
        let self_values = self.values.read();
        let other_values = other.values.read();

        // Compare the contents of the vectors
        *self_values == *other_values
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
        assert_eq!(
            values.len(),
            schema.get_column_count() as usize,
            "Values length does not match schema column count"
        );

        Self {
            values: Arc::new(RwLock::new(values.to_vec())),
            rid,
        }
    }

    /// Serializes the tuple into the given storage buffer.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if serialization fails or if the buffer is too small.
    pub fn serialize_to(&self, storage: &mut [u8]) -> Result<usize, TupleError> {
        let config = config::standard();
        let serialized = bincode::encode_to_vec(self, config)
            .map_err(|e| TupleError::SerializationError(e.to_string()))?;

        if storage.len() < serialized.len() {
            return Err(TupleError::BufferTooSmall);
        }

        storage[..serialized.len()].copy_from_slice(&serialized);
        Ok(serialized.len())
    }

    /// Deserializes a tuple from the given storage buffer.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if deserialization fails.
    pub fn deserialize_from(storage: &[u8]) -> Result<Self, TupleError> {
        let config = config::standard();
        bincode::decode_from_slice(storage, config)
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
        let config = config::standard();
        let values = self.values.read().clone();
        bincode::encode_to_vec(&(values, self.rid), config)
            .map(|vec| vec.len())
            .map_err(|e| TupleError::SerializationError(e.to_string()))
    }

    /// Returns a reference to the value at the given column index.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds.
    pub fn get_value(&self, column_index: usize) -> Value {
        self.values.read()[column_index].clone()
    }

    pub fn set_values(&mut self, values: Vec<Value>) {
        *self.values.write() = values;
    }

    pub fn get_values(&self) -> Vec<Value> {
        self.values.read().clone()
    }

    pub fn get_values_mut(&mut self) -> RwLockWriteGuard<'_, Vec<Value>> {
        self.values.write()
    }

    /// Returns a reference to the schema of this tuple.
    ///
    /// This method constructs a schema based on the types of values in the tuple.
    pub fn get_schema(&self) -> Schema {
        let values = self.values.read();
        let columns: Vec<Column> = values
            .iter()
            .enumerate()
            .map(|(i, value)| Column::new(&format!("col_{}", i), value.get_type_id()))
            .collect();

        Schema::new(columns)
    }

    /// Creates a new tuple containing only the key attributes.
    pub fn keys_from_tuple(&self, key_attrs: &[usize]) -> Vec<Value> {
        let values = self.values.read();
        key_attrs.iter().map(|&attr| values[attr].clone()).collect()
    }

    /// Returns a string representation of the tuple.
    pub fn to_string(&self, schema: Schema) -> String {
        self.to_string_with_schema(&schema)
    }

    /// Returns a detailed string representation of the tuple.
    pub fn to_string_detailed(&self, schema: Schema) -> String {
        // Currently identical to `to_string`; keep this entrypoint for compatibility.
        self.to_string_with_schema(&schema)
    }

    /// Like `to_string`, but avoids moving/cloning the schema.
    pub fn to_string_with_schema(&self, schema: &Schema) -> String {
        let values = self.values.read();
        values
            .iter()
            .enumerate()
            .map(|(i, value)| {
                let col_name = schema
                    .get_column(i)
                    .map(|col| col.get_name().to_string())
                    .unwrap_or_else(|| format!("Column_{}", i));
                format!("{}: {}", col_name, value)
            })
            .collect::<Vec<String>>()
            .join(", ")
    }

    /// Combines this tuple with another tuple by appending the other tuple's values
    pub fn combine(&self, other: &Tuple) -> Self {
        let mut combined_values = self.get_values();
        combined_values.extend(other.get_values());

        Self {
            values: Arc::new(RwLock::new(combined_values)),
            rid: self.rid, // Keep the left tuple's RID
        }
    }

    /// Gets the number of columns in this tuple
    pub fn get_column_count(&self) -> usize {
        self.values.read().len()
    }
}

impl Display for Tuple {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.to_string_detailed(self.get_schema()))
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
    fn test_tuple_to_string_detailed() {
        let (tuple, _) = create_sample_tuple();
        let schema = create_sample_schema();
        let expected = "id: 1, name: Alice, age: 30, is_student: true";
        assert_eq!(tuple.to_string_detailed(schema), expected);
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
        let keys = tuple.keys_from_tuple(&key_attrs);

        assert_eq!(keys[0], Value::new(1));
        assert_eq!(keys[1], Value::new(30));
    }

    #[test]
    fn test_tuple_to_string() {
        let (tuple, _) = create_sample_tuple();
        let schema = create_sample_schema();
        let expected = "id: 1, name: Alice, age: 30, is_student: true";
        assert_eq!(tuple.to_string(schema), expected);
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
        let config = config::standard();

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

        meta.set_deleted(true);
        assert!(meta.is_deleted());
    }

    #[test]
    fn test_tuple_meta_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let meta = TupleMeta::new(1234567890);
        let config = config::standard();
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
