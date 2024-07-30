use crate::types_db::bigint_type::BigIntType;
use crate::types_db::boolean_type::BooleanType;
use crate::types_db::decimal_type::DecimalType;
use crate::types_db::integer_type::IntegerType;
use crate::types_db::smallint_type::SmallIntType;
use crate::types_db::timestamp_type::TimestampType;
use crate::types_db::tinyint_type::TinyIntType;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{ToValue, Value};
use crate::types_db::varlen_type::VarCharType;
use crate::types_db::vector_type::VectorType;

#[derive(Debug, PartialEq, Eq)]
pub enum CmpBool {
    CmpFalse = 0,
    CmpTrue = 1,
    CmpNull = 2,
}

pub trait Type {
    fn get_type_id(&self) -> TypeId;
    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        match self.get_type_id() {
            TypeId::Invalid => false,
            TypeId::Boolean => true,
            TypeId::TinyInt
            | TypeId::SmallInt
            | TypeId::Integer
            | TypeId::BigInt
            | TypeId::Decimal => match type_id {
                TypeId::TinyInt
                | TypeId::SmallInt
                | TypeId::Integer
                | TypeId::BigInt
                | TypeId::Decimal
                | TypeId::VarChar => true,
                _ => false,
            },
            TypeId::Timestamp => type_id == TypeId::VarChar || type_id == TypeId::Timestamp,
            TypeId::VarChar => match type_id {
                TypeId::Boolean
                | TypeId::TinyInt
                | TypeId::SmallInt
                | TypeId::Integer
                | TypeId::BigInt
                | TypeId::Decimal
                | TypeId::Timestamp
                | TypeId::VarChar => true,
                _ => false,
            },
            _ => type_id == self.get_type_id(),
        }
    }
    fn get_min_value(type_id: TypeId) -> Value
    where
        Self: Sized,
    {
        match type_id {
            TypeId::Boolean => false.to_value(),
            TypeId::TinyInt => i8::MIN.to_value(),
            TypeId::SmallInt => i16::MIN.to_value(),
            TypeId::Integer => i32::MIN.to_value(),
            TypeId::BigInt => i64::MIN.to_value(),
            TypeId::Decimal => f64::MIN.to_value(),
            TypeId::Timestamp => 0_u64.to_value(),
            TypeId::VarChar => "".to_value(),
            _ => panic!(),
        }
    }
    fn get_max_value(type_id: TypeId) -> Value
    where
        Self: Sized,
    {
        match type_id {
            TypeId::Boolean => true.to_value(),
            TypeId::TinyInt => i8::MAX.to_value(),
            TypeId::SmallInt => i16::MAX.to_value(),
            TypeId::Integer => i32::MAX.to_value(),
            TypeId::BigInt => i64::MAX.to_value(),
            TypeId::Decimal => f64::MAX.to_value(),
            TypeId::Timestamp => u64::MAX.to_value(),
            TypeId::VarChar => "".to_value(),
            _ => panic!(),
        }
    }
    fn compare_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_not_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_less_than(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_less_than_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_greater_than(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_greater_than_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn add(&self, _left: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn subtract(&self, _left: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn multiply(&self, _left: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn divide(&self, _left: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn modulo(&self, _left: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn min(&self, _left: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn max(&self, _left: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn sqrt(&self, _val: &Value) -> Value {
        unimplemented!()
    }
    fn operate_null(&self, _val: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn is_zero(&self, _val: &Value) -> bool {
        unimplemented!()
    }
    fn is_inlined(&self, _val: &Value) -> bool {
        unimplemented!()
    }
    fn to_string(&self, _val: &Value) -> String {
        unimplemented!()
    }
    fn serialize_to(&self, val: &Value, storage: &mut [u8]);
    fn deserialize_from(&self, storage: &mut [u8]) -> Value;
    fn copy(&self, _val: &Value) -> Value {
        unimplemented!()
    }
    fn cast_as(&self, _val: &Value, _type_id: TypeId) -> Value {
        unimplemented!()
    }
    fn get_data(&self, _val: &Value) -> &[u8] {
        unimplemented!()
    }
    fn get_storage_size(&self, _val: &Value) -> u32 {
        unimplemented!()
    }
}

pub fn get_instance(type_id: TypeId) -> &'static dyn Type {
    match type_id {
        TypeId::Boolean => &BOOLEAN_TYPE_INSTANCE,
        TypeId::TinyInt => &TINYINT_TYPE_INSTANCE,
        TypeId::SmallInt => &SMALLINT_TYPE_INSTANCE,
        TypeId::Integer => &INTEGER_TYPE_INSTANCE,
        TypeId::BigInt => &BIGINT_TYPE_INSTANCE,
        TypeId::Decimal => &DECIMAL_TYPE_INSTANCE,
        TypeId::VarChar => &VARLEN_TYPE_INSTANCE,
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
        TypeId::VarChar => 0,
        _ => panic!(),
    }
}

pub fn type_id_to_string(type_id: TypeId) -> String {
    match type_id {
        TypeId::Boolean => "Boolean".to_string(),
        TypeId::TinyInt => "TinyInt".to_string(),
        TypeId::SmallInt => "SmallInt".to_string(),
        TypeId::Integer => "Integer".to_string(),
        TypeId::BigInt => "BigInt".to_string(),
        TypeId::Decimal => "Decimal".to_string(),
        TypeId::VarChar => "VarChar".to_string(),
        TypeId::Timestamp => "Timestamp".to_string(),
        TypeId::Vector => "Vector".to_string(),
        TypeId::Invalid => "Invalid".to_string(),
    }
}

// Ensure singleton instances are created for each type
static BOOLEAN_TYPE_INSTANCE: BooleanType = BooleanType;
static TINYINT_TYPE_INSTANCE: TinyIntType = TinyIntType;
static SMALLINT_TYPE_INSTANCE: SmallIntType = SmallIntType;
static INTEGER_TYPE_INSTANCE: IntegerType = IntegerType;
static BIGINT_TYPE_INSTANCE: BigIntType = BigIntType;
static DECIMAL_TYPE_INSTANCE: DecimalType = DecimalType;
static VARLEN_TYPE_INSTANCE: VarCharType = VarCharType;
static TIMESTAMP_TYPE_INSTANCE: TimestampType = TimestampType;
static VECTOR_TYPE_INSTANCE: VectorType = VectorType;
static INVALID_TYPE_INSTANCE: InvalidType = InvalidType;

pub struct InvalidType;

impl Type for InvalidType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Invalid
    }

    fn compare_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_not_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_less_than(&self, _left: &Value, _right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_less_than_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_greater_than(&self, _left: &Value, _right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn compare_greater_than_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        // Implement comparison logic
        unimplemented!()
    }

    fn add(&self, _left: &Value, _right: &Value) -> Value {
        // Implement addition logic
        unimplemented!()
    }

    fn subtract(&self, _left: &Value, _right: &Value) -> Value {
        // Implement subtraction logic
        unimplemented!()
    }

    fn multiply(&self, _left: &Value, _right: &Value) -> Value {
        // Implement multiplication logic
        unimplemented!()
    }

    fn divide(&self, _left: &Value, _right: &Value) -> Value {
        // Implement division logic
        unimplemented!()
    }

    fn modulo(&self, _left: &Value, _right: &Value) -> Value {
        // Implement modulo logic
        unimplemented!()
    }

    fn min(&self, _left: &Value, _right: &Value) -> Value {
        // Implement min logic
        unimplemented!()
    }

    fn max(&self, _left: &Value, _right: &Value) -> Value {
        // Implement max logic
        unimplemented!()
    }

    fn sqrt(&self, _val: &Value) -> Value {
        // Implement sqrt logic
        unimplemented!()
    }

    fn operate_null(&self, _val: &Value, _right: &Value) -> Value {
        // Implement null operation logic
        unimplemented!()
    }

    fn is_zero(&self, _val: &Value) -> bool {
        // Implement is_zero logic
        unimplemented!()
    }

    fn is_inlined(&self, _val: &Value) -> bool {
        // Implement is_inlined logic
        unimplemented!()
    }

    fn to_string(&self, _val: &Value) -> String {
        // Implement to_string logic
        unimplemented!()
    }

    fn serialize_to(&self, _val: &Value, _storage: &mut [u8]) {
        unimplemented!()
    }

    fn deserialize_from(&self, _storage: &mut [u8]) -> Value {
        unimplemented!()
    }

    fn copy(&self, _val: &Value) -> Value {
        // Implement copy logic
        unimplemented!()
    }

    fn cast_as(&self, _val: &Value, _type_id: TypeId) -> Value {
        // Implement cast logic
        unimplemented!()
    }

    fn get_data(&self, _val: &Value) -> &[u8] {
        // Implement get_data logic
        unimplemented!()
    }

    fn get_storage_size(&self, _val: &Value) -> u32 {
        // Implement get_storage_size logic
        unimplemented!()
    }
}
