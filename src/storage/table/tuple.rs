use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::config::{Timestamp, TxnId};
use crate::common::exception::TupleError;
use crate::common::rid::RID;
use crate::concurrency::watermark::Watermark;
use crate::types_db::value::Value;
use bincode;
use log;
use serde::{Deserialize, Serialize};
use std::fmt::{Display, Formatter};

/// Metadata associated with a tuple.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Copy)]
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
            self.creator_txn_id, txn_id, self.commit_timestamp, self.deleted, watermark.get_watermark()
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
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize, Eq)]
pub struct Tuple {
    values: Vec<Value>,
    rid: RID,
}

impl Tuple {
    /// Creates a new `Tuple` instance.
    ///
    /// # Panics
    ///
    /// Panics if the number of values doesn't match the schema's column count.
    pub fn new(values: &[Value], schema: Schema, rid: RID) -> Self {
        assert_eq!(
            values.len(),
            schema.get_column_count() as usize,
            "Values length does not match schema column count"
        );

        Self {
            values: values.to_vec(),
            rid,
        }
    }

    /// Serializes the tuple into the given storage buffer.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if serialization fails or if the buffer is too small.
    pub fn serialize_to(&self, storage: &mut [u8]) -> Result<usize, TupleError> {
        let serialized = bincode::serialize(&(self.values.clone(), self.rid))
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
        let (values, rid): (Vec<Value>, RID) = bincode::deserialize(storage)
            .map_err(|e| TupleError::DeserializationError(e.to_string()))?;
        Ok(Self { values, rid })
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
        bincode::serialized_size(&(self.values.clone(), self.rid))
            .map(|size| size as usize)
            .map_err(|e| TupleError::SerializationError(e.to_string()))
    }

    /// Returns a reference to the value at the given column index.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds.
    pub fn get_value(&self, column_index: usize) -> &Value {
        &self.values[column_index]
    }

    pub fn set_values(&mut self, values: Vec<Value>) {
        self.values = values;
    }

    pub fn get_values(&self) -> &Vec<Value> {
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
            .map(|(i, value)| {
                Column::new(
                    &format!("col_{}", i), // Default column name
                    value.get_type_id(),   // Get TypeId from the Value
                )
            })
            .collect();

        Schema::new(columns)
    }

    /// Creates a new tuple containing only the key attributes.
    pub fn keys_from_tuple(&self, key_attrs: Vec<usize>) -> Vec<Value> {
        let key_values: Vec<Value> = key_attrs
            .iter()
            .map(|&attr| self.get_value(attr).clone())
            .collect();
        key_values
    }

    /// Returns a string representation of the tuple.
    pub fn to_string(&self, schema: Schema) -> String {
        self.values
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

    /// Returns a detailed string representation of the tuple.
    pub fn to_string_detailed(&self, schema: Schema) -> String {
        self.values
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
        let mut combined_values = self.values.clone();
        combined_values.extend(other.values.clone());

        Self {
            values: combined_values,
            rid: self.rid, // Keep the left tuple's RID
        }
    }

    /// Gets the number of columns in this tuple
    pub fn get_column_count(&self) -> usize {
        self.values.len()
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
        (Tuple::new(&*values, schema.clone(), rid), schema)
    }

    #[test]
    fn test_tuple_creation_and_access() {
        let (tuple, _) = create_sample_tuple();

        assert_eq!(tuple.get_value(0), &Value::new(1));
        assert_eq!(tuple.get_value(1), &Value::new("Alice"));
        assert_eq!(tuple.get_value(2), &Value::new(30));
        assert_eq!(tuple.get_value(3), &Value::new(true));
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
        let keys = tuple.keys_from_tuple(key_attrs);

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
        Tuple::new(&*values, schema, rid);
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
    fn test_tuple_meta() {
        let mut meta = TupleMeta::new(1);
        assert_eq!(meta.get_creator_txn_id(), 1);
        assert_eq!(meta.is_committed(), false);
        assert_eq!(meta.is_deleted(), false);

        meta.set_deleted(true);
        assert_eq!(meta.is_deleted(), true);

        // Test copy semantics
        let meta2 = meta;
        assert_eq!(meta2.get_creator_txn_id(), 1);
        assert_eq!(meta2.is_committed(), false);
        assert_eq!(meta2.is_deleted(), true);

        // Both meta and meta2 should be usable since meta was copied, not moved
        assert_eq!(meta.get_creator_txn_id(), 1);
        assert_eq!(meta.is_committed(), false);
        assert_eq!(meta.is_deleted(), true);

        // Modifying one shouldn't affect the other
        let mut meta3 = meta;
        meta3.set_deleted(false);
        assert_eq!(meta3.is_deleted(), false);
        assert_eq!(meta.is_deleted(), true);
    }

    #[test]
    fn test_tuple_meta_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let meta = TupleMeta::new(1234567890);
        let serialized = bincode::serialize(&meta)?;
        let deserialized: TupleMeta = bincode::deserialize(&serialized)?;

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
            Value::new(3.14),
            Value::new("Hello"),
            Value::new(true),
        ];
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*values, schema, rid);

        assert_eq!(tuple.get_value(0), &Value::new(42));
        assert_eq!(tuple.get_value(1), &Value::new(3.14));
        assert_eq!(tuple.get_value(2), &Value::new("Hello"));
        assert_eq!(tuple.get_value(3), &Value::new(true));
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
        let tuple = Tuple::new(&values, schema.clone(), rid);

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
