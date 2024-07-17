use std::mem::size_of;
use std::ptr::null;
use limits::BUSTUB_VALUE_NULL;
use value::Value;
use schema::Schema;
use type_id::TypeId;

pub struct Tuple {
    values: Vec<Value>,
    schema: Schema,
    rid: u32,
    data: Vec<char>
}

impl Tuple {
    pub fn new(values: Vec<Value>, schema: &Schema) {
        assert_eq!(values.len(), schema.get_column_count(), "Values length does not match schema column count");

        let mut tuple_size = schema.get_inlined_storage_size() as usize;
        for value in &values {
            let mut len = value.get_storage_size();
            if len == BUSTUB_VALUE_NULL {
                len = 0;
            }
            tuple_size += size_of::<u32>() + len as usize;
        }
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
