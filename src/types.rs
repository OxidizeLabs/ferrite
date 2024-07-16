use bigint_type::BigIntType;
use boolean_type::BooleanType;
use decimal_type::DecimalType;
use integer_type::IntegerType;
use smallint_type::SmallIntType;
use timestamp_type::TimestampType;
use tinyint_type::TinyIntType;
use type_id::TypeId;
use value::Value;
use varlen_type::VarcharType;
use vector_type::VectorType;


#[derive(Debug, PartialEq, Eq)]
pub enum CmpBool {
    CmpFalse = 0,
    CmpTrue = 1,
    CmpNull = 2,
}

pub trait Type {
    fn get_type_size(&self) -> u64;
    fn is_coercible_from(&self, type_id: TypeId) -> bool;
    fn type_id_to_string(&self) -> String;
    fn get_min_value(&self) -> Value;
    fn get_max_value(&self) -> Value;
    fn get_type_id(&self) -> TypeId;
    fn compare_equals(&self, left: &Value, right: &Value) -> CmpBool;
    fn compare_not_equals(&self, left: &Value, right: &Value) -> CmpBool;
    fn compare_less_than(&self, left: &Value, right: &Value) -> CmpBool;
    fn compare_less_than_equals(&self, left: &Value, right: &Value) -> CmpBool;
    fn compare_greater_than(&self, left: &Value, right: &Value) -> CmpBool;
    fn compare_greater_than_equals(&self, left: &Value, right: &Value) -> CmpBool;
    fn add(&self, left: &Value, right: &Value) -> Value;
    fn subtract(&self, left: &Value, right: &Value) -> Value;
    fn multiply(&self, left: &Value, right: &Value) -> Value;
    fn divide(&self, left: &Value, right: &Value) -> Value;
    fn modulo(&self, left: &Value, right: &Value) -> Value;
    fn min(&self, left: &Value, right: &Value) -> Value;
    fn max(&self, left: &Value, right: &Value) -> Value;
    fn sqrt(&self, val: &Value) -> Value;
    fn operate_null(&self, val: &Value, right: &Value) -> Value;
    fn is_zero(&self, val: &Value) -> bool;
    fn is_inlined(&self, val: &Value) -> bool;
    fn to_string(&self, val: &Value) -> String;
    fn serialize_to(&self, val: &Value, storage: &mut [u8]);
    fn deserialize_from(&self, storage: &[u8]) -> Value;
    fn copy(&self, val: &Value) -> Value;
    fn cast_as(&self, val: &Value, type_id: TypeId) -> Value;
    fn get_data(&self, val: &Value) -> &[u8];
    fn get_storage_size(&self, val: &Value) -> u32;
}

pub fn get_instance(type_id: TypeId) -> &'static dyn Type {
    match type_id {
        TypeId::Boolean => &BOOLEAN_TYPE_INSTANCE,
        TypeId::TinyInt => &TINYINT_TYPE_INSTANCE,
        TypeId::SmallInt => &SMALLINT_TYPE_INSTANCE,
        TypeId::Integer => &INTEGER_TYPE_INSTANCE,
        TypeId::BigInt => &BIGINT_TYPE_INSTANCE,
        TypeId::Decimal => &DECIMAL_TYPE_INSTANCE,
        TypeId::Varchar => &VARLEN_TYPE_INSTANCE,
        TypeId::Timestamp => &TIMESTAMP_TYPE_INSTANCE,
        TypeId::Vector => &VECTOR_TYPE_INSTANCE,
        TypeId::Invalid => &INVALID_TYPE_INSTANCE,
    }
}

pub fn get_type_size(type_id: TypeId) -> u64 {
    match type_id {
        TypeId::Boolean | TypeId::TinyInt => 1,
        TypeId::SmallInt => 2,
        TypeId::Integer => 4,
        TypeId::BigInt | TypeId::Decimal | TypeId::Timestamp => 8,
        TypeId::Varchar => 0,
        _ => panic!()
    }
}

// Ensure singleton instances are created for each type
static BOOLEAN_TYPE_INSTANCE: BooleanType = BooleanType;
static TINYINT_TYPE_INSTANCE: TinyIntType = TinyIntType;
static SMALLINT_TYPE_INSTANCE: SmallIntType = SmallIntType;
static INTEGER_TYPE_INSTANCE: IntegerType = IntegerType;
static BIGINT_TYPE_INSTANCE: BigIntType = BigIntType;
static DECIMAL_TYPE_INSTANCE: DecimalType = DecimalType;
static VARLEN_TYPE_INSTANCE: VarcharType = VarcharType;
static TIMESTAMP_TYPE_INSTANCE: TimestampType = TimestampType;
static VECTOR_TYPE_INSTANCE: VectorType = VectorType;
static INVALID_TYPE_INSTANCE: InvalidType = InvalidType;

pub struct InvalidType;

impl Type for InvalidType {
    fn get_type_size(&self) -> u64 {
        1 // Example size
    }

    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        matches!(type_id, TypeId::Invalid)
    }

    fn type_id_to_string(&self) -> String {
        "Invalid".to_string()
    }

    fn get_min_value(&self) -> Value {
        // Return the minimum value for a boolean type
        unimplemented!()
    }

    fn get_max_value(&self) -> Value {
        // Return the maximum value for a boolean type
        unimplemented!()
    }

    fn get_type_id(&self) -> TypeId {
        TypeId::Invalid
    }

    fn compare_equals(&self, left: &Value, right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_not_equals(&self, left: &Value, right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_less_than(&self, left: &Value, right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_less_than_equals(&self, left: &Value, right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_greater_than(&self, left: &Value, right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_greater_than_equals(&self, left: &Value, right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn add(&self, left: &Value, right: &Value) -> Value {
        // Implement addition logic
        unimplemented!()
    }

    fn subtract(&self, left: &Value, right: &Value) -> Value {
        // Implement subtraction logic
        unimplemented!()
    }

    fn multiply(&self, left: &Value, right: &Value) -> Value {
        // Implement multiplication logic
        unimplemented!()
    }

    fn divide(&self, left: &Value, right: &Value) -> Value {
        // Implement division logic
        unimplemented!()
    }

    fn modulo(&self, left: &Value, right: &Value) -> Value {
        // Implement modulo logic
        unimplemented!()
    }

    fn min(&self, left: &Value, right: &Value) -> Value {
        // Implement min logic
        unimplemented!()
    }

    fn max(&self, left: &Value, right: &Value) -> Value {
        // Implement max logic
        unimplemented!()
    }

    fn sqrt(&self, val: &Value) -> Value {
        // Implement sqrt logic
        unimplemented!()
    }

    fn operate_null(&self, val: &Value, right: &Value) -> Value {
        // Implement null operation logic
        unimplemented!()
    }

    fn is_zero(&self, val: &Value) -> bool {
        // Implement is_zero logic
        unimplemented!()
    }

    fn is_inlined(&self, val: &Value) -> bool {
        // Implement is_inlined logic
        unimplemented!()
    }

    fn to_string(&self, val: &Value) -> String {
        // Implement to_string logic
        unimplemented!()
    }

    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        // Implement serialization logic
        unimplemented!()
    }

    fn deserialize_from(&self, storage: &[u8]) -> Value {
        // Implement deserialization logic
        unimplemented!()
    }

    fn copy(&self, val: &Value) -> Value {
        // Implement copy logic
        unimplemented!()
    }

    fn cast_as(&self, val: &Value, type_id: TypeId) -> Value {
        // Implement cast logic
        unimplemented!()
    }

    fn get_data(&self, val: &Value) -> &[u8] {
        // Implement get_data logic
        unimplemented!()
    }

    fn get_storage_size(&self, val: &Value) -> u32 {
        // Implement get_storage_size logic
        unimplemented!()
    }
}
