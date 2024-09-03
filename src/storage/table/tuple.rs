use serde::{Serialize, Deserialize, Serializer, Deserializer};
use bincode::{serialize, deserialize, serialized_size};
use crate::catalogue::schema::Schema;
use crate::common::exception::TupleError;
use crate::types_db::value::Value;
use crate::types_db::type_id::TypeId;
use crate::catalogue::column::Column;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TupleMeta {
    timestamp: u64,
    is_deleted: bool,
}

#[derive(Clone, Debug)]
pub struct Tuple {
    values: Vec<Value>,
    schema: Schema,
    rid: u32,
}

impl TupleMeta {
    pub fn new(timestamp: u64, is_deleted: bool) -> Self {
        Self {
            timestamp,
            is_deleted,
        }
    }
}

impl Tuple {
    pub fn new(values: Vec<Value>, schema: Schema, rid: u32) -> Self {
        assert_eq!(
            values.len(),
            schema.get_column_count() as usize,
            "Values length does not match schema column count"
        );

        Self {
            values,
            schema,
            rid,
        }
    }

    pub fn serialize_to(&self, storage: &mut [u8]) -> Result<usize, TupleError> {
        let serialized = serialize(self)?;
        if storage.len() < serialized.len() {
            return Err(TupleError::BufferTooSmall);
        }
        storage[..serialized.len()].copy_from_slice(&serialized);
        Ok(serialized.len())
    }

    pub fn deserialize_from(storage: &[u8], schema: Schema) -> Result<Self, TupleError> {
        let mut tuple: Tuple = deserialize(storage)?;
        tuple.schema = schema;
        Ok(tuple)
    }

    pub fn get_rid(&self) -> u32 {
        self.rid
    }

    pub fn set_rid(&mut self, rid: u32) {
        self.rid = rid;
    }

    pub fn get_length(&self) -> Result<usize, TupleError> {
        Ok(serialized_size(self)? as usize)
    }

    pub fn get_value(&self, column_index: usize) -> &Value {
        &self.values[column_index]
    }

    pub fn key_from_tuple(&self, key_schema: Schema, key_attrs: Vec<u32>) -> Tuple {
        let key_values: Vec<Value> = key_attrs.iter()
            .map(|&attr| self.get_value(attr as usize).clone())
            .collect();

        Tuple::new(key_values, key_schema, self.rid)
    }

    pub fn to_string(&self) -> String {
        self.values.iter().enumerate()
            .map(|(i, value)| {
                let col_name = self.schema.get_column(i)
                    .map(|col| col.get_name().to_string())
                    .unwrap_or_else(|| format!("Column_{}", i));
                format!("{}: {}", col_name, value)
            })
            .collect::<Vec<String>>()
            .join(", ")
    }
}

impl Serialize for Tuple {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("Tuple", 2)?;
        state.serialize_field("values", &self.values)?;
        state.serialize_field("rid", &self.rid)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Tuple {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::{self, Visitor, SeqAccess, MapAccess};

        struct TupleVisitor;

        impl<'de> Visitor<'de> for TupleVisitor {
            type Value = Tuple;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("struct Tuple")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Tuple, V::Error>
            where
                V: SeqAccess<'de>,
            {
                let values = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let rid = seq.next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;

                Ok(Tuple {
                    values,
                    schema: Schema::default(), // We'll set this later
                    rid,
                })
            }

            fn visit_map<V>(self, mut map: V) -> Result<Tuple, V::Error>
            where
                V: MapAccess<'de>,
            {
                let mut values = None;
                let mut rid = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        "values" => {
                            if values.is_some() {
                                return Err(de::Error::duplicate_field("values"));
                            }
                            values = Some(map.next_value()?);
                        }
                        "rid" => {
                            if rid.is_some() {
                                return Err(de::Error::duplicate_field("rid"));
                            }
                            rid = Some(map.next_value()?);
                        }
                        _ => {
                            return Err(de::Error::unknown_field(key, &["values", "rid"]));
                        }
                    }
                }
                let values = values.ok_or_else(|| de::Error::missing_field("values"))?;
                let rid = rid.ok_or_else(|| de::Error::missing_field("rid"))?;
                Ok(Tuple {
                    values,
                    schema: Schema::default(), // We'll set this later
                    rid,
                })
            }
        }

        deserializer.deserialize_struct("Tuple", &["values", "rid"], TupleVisitor)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn tuple_serialization() -> Result<(), TupleError> {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let values = vec![
            Value::new(42),
            Value::new("Alice"),
        ];
        let tuple = Tuple::new(values, schema.clone(), 1);

        let mut storage = vec![0u8; 1000];
        let serialized_len = tuple.serialize_to(&mut storage)?;

        let deserialized = Tuple::deserialize_from(&storage[..serialized_len], schema)?;
        assert_eq!(deserialized.get_rid(), 1);
        assert_eq!(deserialized.get_value(0), &Value::new(42));
        assert_eq!(deserialized.get_value(1), &Value::new("Alice"));

        Ok(())
    }

    #[test]
    fn tuple_key_from_tuple() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);
        let values = vec![
            Value::new(42),
            Value::new("Alice"),
            Value::new(30),
        ];
        let tuple = Tuple::new(values, schema.clone(), 1);

        let key_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);
        let key_attrs = vec![0, 2];
        let key_tuple = tuple.key_from_tuple(key_schema, key_attrs);

        assert_eq!(key_tuple.get_value(0), &Value::new(42));
        assert_eq!(key_tuple.get_value(1), &Value::new(30));
    }
}