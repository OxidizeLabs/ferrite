use crate::catalog::schema::Schema;
use crate::common::exception::TupleError;
use crate::common::rid::RID;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt::{Display, Formatter};

/// Represents a tuple that has been stored in the database with a record ID.
#[derive(Debug)]
#[allow(dead_code)]
pub struct Record {
    tuple: Tuple,
    rid: RID,
}

// Implement bincode's Encode trait for Record
impl bincode::Encode for Record {
    fn encode<E: bincode::enc::Encoder>(&self, encoder: &mut E) -> Result<(), bincode::error::EncodeError> {
        let values = self.tuple.get_values();
        (values, self.rid).encode(encoder)
    }
}

// Implement bincode's Decode trait for Record
impl<C> bincode::Decode<C> for Record {
    fn decode<D: bincode::de::Decoder<Context = C>>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError> {
        let (values, rid): (Vec<Value>, RID) = bincode::Decode::decode(decoder)?;
        // Create a schema from the values - generate column names and types
        let columns = values.iter().enumerate().map(|(i, value)| {
            use crate::catalog::column::Column;
            Column::new(&format!("col_{}", i), value.get_type_id())
        }).collect();
        let schema = Schema::new(columns);
        let tuple = Tuple::new(&values, &schema, rid);
        Ok(Self { tuple, rid })
    }
}

// Provide a generic BorrowDecode implementation so Record works with bincode decode derives
impl<'de, C> bincode::BorrowDecode<'de, C> for Record {
    fn borrow_decode<D>(decoder: &mut D) -> Result<Self, bincode::error::DecodeError>
    where
        D: bincode::de::BorrowDecoder<'de, Context = C>,
    {
        let (values, rid): (Vec<Value>, RID) = bincode::BorrowDecode::borrow_decode(decoder)?;
        // Create a schema from the values - generate column names and types
        let columns = values.iter().enumerate().map(|(i, value)| {
            use crate::catalog::column::Column;
            Column::new(&format!("col_{}", i), value.get_type_id())
        }).collect();
        let schema = Schema::new(columns);
        let tuple = Tuple::new(&values, &schema, rid);
        Ok(Self { tuple, rid })
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
    pub fn new(tuple: Tuple, rid: RID) -> Self {
        Self { tuple, rid }
    }

    /// Serializes the record into the given storage buffer using bincode 2.0.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if serialization fails or if the buffer is too small.
    pub fn serialize_to(&self, storage: &mut [u8]) -> Result<usize, TupleError> {
        let config = bincode::config::standard();
        let serialized = bincode::encode_to_vec(self, config)
            .map_err(|e| TupleError::SerializationError(e.to_string()))?;

        if storage.len() < serialized.len() {
            return Err(TupleError::BufferTooSmall);
        }

        storage[..serialized.len()].copy_from_slice(&serialized);
        Ok(serialized.len())
    }

    /// Deserializes a record from the given storage buffer using bincode 2.0.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if deserialization fails.
    pub fn deserialize_from(storage: &[u8]) -> Result<Self, TupleError> {
        let config = bincode::config::standard();
        let (record, _): (Self, usize) = bincode::decode_from_slice(storage, config)
            .map_err(|e| TupleError::DeserializationError(e.to_string()))?;
        Ok(record)
    }

    /// Returns the RID of the record.
    pub fn get_rid(&self) -> RID {
        self.rid
    }

    /// Sets the RID of the record.
    pub fn set_rid(&mut self, rid: RID) {
        self.rid = rid;
    }

    /// Returns the length of the serialized record using bincode 2.0.
    ///
    /// # Errors
    ///
    /// Returns a `TupleError` if serialization fails.
    pub fn get_length(&self) -> Result<usize, TupleError> {
        let config = bincode::config::standard();
        let values = self.tuple.get_values();
        let serialized = bincode::encode_to_vec(&(values, self.rid), config)
            .map_err(|e| TupleError::SerializationError(e.to_string()))?;
        Ok(serialized.len())
    }

    /// Convenience method to get a value directly from the record.
    pub fn get_value(&self, column_index: usize) -> Value {
        self.tuple.get_value(column_index)
    }

    /// Returns a detailed string representation of the record.
    pub fn to_string_detailed(&self, schema: Schema) -> String {
        format!(
            "RID: {}, {}",
            self.rid,
            self.tuple.to_string_detailed(schema)
        )
    }
}

impl Display for Record {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RID: {}, {}",
            self.rid,
            self.tuple.to_string_detailed(self.tuple.get_schema())
        )
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

    fn create_sample_record() -> (Record, Schema) {
        let (tuple, schema) = create_sample_tuple();
        let rid = RID::new(0, 0);
        (Record::new(tuple, rid), schema)
    }

    #[test]
    fn test_record_creation_and_access() {
        let (record, _) = create_sample_record();

        assert_eq!(record.get_value(0), Value::new(1));
        assert_eq!(record.get_value(1), Value::new("Alice"));
        assert_eq!(record.get_value(2), Value::new(30));
        assert_eq!(record.get_value(3), Value::new(true));
        assert_eq!(record.get_rid(), RID::new(0, 0));
    }

    #[test]
    fn test_record_to_string_detailed() {
        let (record, schema) = create_sample_record();
        let expected = "RID: page_id: 0 slot_num: 0, id: 1, name: Alice, age: 30, is_student: true";
        assert_eq!(record.to_string_detailed(schema), expected);
    }

    #[test]
    fn test_record_serialization_deserialization() -> Result<(), TupleError> {
        let (record, _schema) = create_sample_record();

        let mut storage = vec![0u8; 1000];
        let serialized_len = record.serialize_to(&mut storage)?;

        let deserialized = Record::deserialize_from(&storage[..serialized_len])?;
        assert_eq!(deserialized.get_rid(), record.get_rid());
        assert_eq!(deserialized.get_value(0), record.get_value(0));
        assert_eq!(deserialized.get_value(1), record.get_value(1));
        assert_eq!(deserialized.get_value(2), record.get_value(2));
        assert_eq!(deserialized.get_value(3), record.get_value(3));

        Ok(())
    }

    #[test]
    fn test_record_set_rid() {
        let (mut record, _) = create_sample_record();
        let new_rid = RID::new(1, 1);
        record.set_rid(new_rid);
        assert_eq!(record.get_rid(), new_rid);
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

        let config = bincode::config::standard();
        // Directly use bincode 2.0 API
        let serialized = bincode::encode_to_vec(&record, config)?;

        // Directly use bincode 2.0 API for deserialization
        let (deserialized, _): (Record, usize) = bincode::decode_from_slice(&serialized, config)?;

        // Verify the deserialized record matches the original
        assert_eq!(deserialized.get_rid(), record.get_rid());
        assert_eq!(deserialized.get_value(0), record.get_value(0));
        assert_eq!(deserialized.get_value(1), record.get_value(1));
        assert_eq!(deserialized.get_value(2), record.get_value(2));
        assert_eq!(deserialized.get_value(3), record.get_value(3));

        Ok(())
    }
}
