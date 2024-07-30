use std::mem::size_of;

use crate::catalogue::schema::Schema;
use crate::types_db::limits::DB_VALUE_NULL;
use crate::types_db::value::Value;

#[derive(Clone, Debug)]
pub struct TupleMeta {
    timestamp: u64,
    is_deleted: bool
}

#[derive(Clone, Debug)]
pub struct Tuple {
    values: Vec<Value>,
    schema: Schema,
    rid: u32,
    data: Vec<u8>,
}

impl TupleMeta {
    pub fn new(timestamp: u64, is_deleted: bool) -> Self {
        Self {
            timestamp,
            is_deleted
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

        // 1. Calculate the size of the tuple.
        let mut tuple_size = schema.get_inlined_storage_size() as usize;
        for &i in schema.get_unlined_columns() {
            let mut len = values[i as usize].get_storage_size();
            if len == DB_VALUE_NULL {
                len = 0;
            }
            tuple_size += size_of::<u32>() + len as usize;
        }

        // 2. Allocate memory.
        let mut data = vec![0; tuple_size];

        // 3. Serialize each attribute based on the input value.
        let mut offset = schema.get_inlined_storage_size() as usize;

        for (i, value) in values.iter().enumerate() {
            if let Some(col) = schema.get_column(i) {
                if !col.is_inlined() {
                    // Serialize relative offset, where the actual varchar data is stored.
                    let offset_ptr = &mut data
                        [col.get_offset() as usize..col.get_offset() as usize + size_of::<u32>()];
                    offset_ptr.copy_from_slice(&(offset as u32).to_ne_bytes());

                    // Serialize varchar value, in place (size + data).
                    value.serialize_to(&mut data[offset..]);
                    let mut len = value.get_storage_size();
                    if len == DB_VALUE_NULL {
                        len = 0;
                    }
                    offset += size_of::<u32>() + len as usize;
                } else {
                    value.serialize_to(&mut data[col.get_offset() as usize..]);
                }
            } else {
                panic!("Column at index {} not found", i);
            }
        }

        Tuple {
            values,
            schema,
            rid,
            data,
        }
    }

    pub fn serialize_to(&self, storage: &mut [u8]) {
        assert!(
            storage.len() >= self.data.len(),
            "Storage buffer is too small"
        );
        storage[..self.data.len()].copy_from_slice(&self.data);
    }

    pub fn deserialize_from(storage: &[u8], schema: Schema, rid: u32) -> Self {
        let mut values = Vec::new();

        for (_i, col) in schema.get_columns().iter().enumerate() {
            if col.is_inlined() {
                let val =
                    Value::deserialize_from(&storage[col.get_offset() as usize..], col.get_type());
                values.push(val);
            } else {
                let offset = u32::from_ne_bytes([
                    storage[col.get_offset() as usize],
                    storage[col.get_offset() as usize + 1],
                    storage[col.get_offset() as usize + 2],
                    storage[col.get_offset() as usize + 3],
                ]) as usize;
                let val = Value::deserialize_from(&storage[offset..], col.get_type());
                values.push(val);
            }
        }

        Tuple {
            values,
            schema,
            rid,
            data: storage.to_vec(),
        }
    }

    pub fn get_rid(&self) -> u32 {
        self.rid
    }

    pub fn set_rid(&mut self, rid: u32) {
        self.rid = rid;
    }

    pub fn get_data(&self) -> &[u8] {
        &self.data
    }

    pub fn get_length(&self) -> u32 {
        self.data.len() as u32
    }

    pub fn get_value(&self, column_index: usize) -> &Value {
        &self.values[column_index]
    }

    pub fn key_from_tuple(&self, key_schema: Schema, key_attrs: Vec<u32>) -> Tuple {
        let mut key_values = Vec::new();

        for &attr in &key_attrs {
            key_values.push(self.get_value(attr as usize).clone());
        }

        Tuple::new(key_values, key_schema, self.rid)
    }

    pub fn to_string(&self, schema: &Schema) -> String {
        let mut output = String::new();
        for (i, value) in self.values.iter().enumerate() {
            if let Some(col) = schema.get_column(i) {
                output.push_str(&format!("{}: {}, ", col.get_name(), value.to_string()));
            }
        }
        output.pop(); // Remove last comma
        output.pop(); // Remove last space
        output
    }

    fn get_data_ptr(&self, column_index: u32) -> &[u8] {
        let col = self.schema.get_column(column_index as usize).unwrap();
        if col.is_inlined() {
            &self.data[col.get_offset() as usize..]
        } else {
            let offset = u32::from_ne_bytes([
                self.data[col.get_offset() as usize],
                self.data[col.get_offset() as usize + 1],
                self.data[col.get_offset() as usize + 2],
                self.data[col.get_offset() as usize + 3],
            ]) as usize;
            &self.data[offset..]
        }
    }
}
