use std::mem::size_of;
use limits::DB_VALUE_NULL;
use value::Value;
use schema::Schema;

pub struct Tuple {
    values: Vec<Value>,
    schema: Schema,
    rid: u32,
    data: Vec<u8>,
}

impl Tuple {
    pub fn new(values: Vec<Value>, schema: Schema, rid: u32) -> Self {
        assert_eq!(
            values.len(),
            schema.get_column_count(),
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
                    let offset_ptr = &mut data[col.get_offset() as usize..col.get_offset() as usize + size_of::<u32>()];
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

        Tuple { values, schema, rid, data }
    }


    pub fn serialize_to() {
        unimplemented!()
    }

    pub fn deserialize_from() {
        unimplemented!()
    }

    pub fn get_rid(&self) -> u32 {
        self.rid
    }

    pub fn set_rid(&self, rid: u32) {
        unimplemented!()
    }

    pub fn get_data(&self) {
        unimplemented!()
    }

    pub fn get_length(&self) -> u32 {
        unimplemented!()
    }

    pub fn get_value(&self, schema: &Schema, column_index: u32) -> Value {
        unimplemented!()
    }

    pub fn key_from_tuple(&self, schema: &Schema, key_schema: Schema, key_attrs: Vec<u32>) -> Tuple {
        unimplemented!()
    }

    // pub fn is_null(&self, schema: &Schema, column_index: u32) -> bool {
    //     let value = Value::GetValue(schema, column_index);
    //     return value.IsNull();
    // }

    pub fn to_string(&self, schema: &Schema) -> String {
        unimplemented!()
    }

    fn get_data_ptr(&self, schema: Schema, column_index: u32) {
        unimplemented!()
    }
}
