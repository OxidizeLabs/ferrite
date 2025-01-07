use crate::catalogue::schema::Schema;
use crate::common::exception::TupleError;
use crate::common::rid::RID;
use crate::types_db::value::Value;
use bincode;
use serde::{Deserialize, Serialize};

/// Metadata associated with a tuple.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TupleMeta {
    timestamp: u64,
    is_deleted: bool,
}

impl TupleMeta {
    /// Creates a new `TupleMeta` instance.
    pub fn new(timestamp: u64, is_deleted: bool) -> Self {
        Self {
            timestamp,
            is_deleted,
        }
    }

    /// Returns the timestamp of the tuple.
    pub fn get_timestamp(&self) -> u64 {
        self.timestamp
    }

    /// Returns whether the tuple is marked as deleted.
    pub fn is_deleted(&self) -> bool {
        self.is_deleted
    }

    pub fn mark_as_deleted(&mut self) {
        self.is_deleted = true
    }
}

/// Represents a tuple in the database.
#[derive(Clone, Debug, PartialEq, PartialOrd, Serialize, Deserialize)]
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

    pub fn get_values(&self) -> &Vec<Value> {
        &self.values
    }

    /// Creates a new tuple containing only the key attributes.
    pub fn key_from_tuple(&self, key_schema: Schema, key_attrs: Vec<usize>) -> Tuple {
        let key_values: Vec<Value> = key_attrs
            .iter()
            .map(|&attr| self.get_value(attr).clone())
            .collect();

        Tuple::new(&*key_values, key_schema, self.rid)
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
                format!("{}: {:#}", col_name, value)
            })
            .collect::<Vec<String>>()
            .join(", ")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::column::Column;
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
        let expected =
            "id: Integer(1), name: VarLen(\"Alice\"), age: Integer(30), is_student: Boolean(true)";
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
    fn test_tuple_key_from_tuple() {
        let (tuple, _schema) = create_sample_tuple();

        let key_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);
        let key_attrs = vec![0, 2];
        let key_tuple = tuple.key_from_tuple(key_schema.clone(), key_attrs);

        assert_eq!(key_tuple.get_value(0), &Value::new(1));
        assert_eq!(key_tuple.get_value(1), &Value::new(30));
        assert_eq!(key_tuple.get_rid(), tuple.get_rid());
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
        let meta = TupleMeta::new(1234567890, false);
        assert_eq!(meta.get_timestamp(), 1234567890);
        assert_eq!(meta.is_deleted(), false);

        let meta_deleted = TupleMeta::new(9876543210, true);
        assert_eq!(meta_deleted.get_timestamp(), 9876543210);
        assert_eq!(meta_deleted.is_deleted(), true);
    }

    #[test]
    fn test_tuple_meta_serialization() -> Result<(), Box<dyn std::error::Error>> {
        let meta = TupleMeta::new(1234567890, false);
        let serialized = bincode::serialize(&meta)?;
        let deserialized: TupleMeta = bincode::deserialize(&serialized)?;

        assert_eq!(meta.get_timestamp(), deserialized.get_timestamp());
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
}
