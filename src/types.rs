pub mod boolean_type;
pub mod tinyint_type;
pub mod smallint_type;
pub mod integer_type;
pub mod bigint_type;
pub mod decimal_type;
pub mod varlen_type;
pub mod timestamp_type;
pub mod vector_type;

use crate::value::Value;
use crate::type_id::TypeId;
use crate::boolean_type::BooleanType;


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
        // TypeId::TinyInt => &TINYINT_TYPE_INSTANCE,
        // TypeId::SmallInt => &SMALLINT_TYPE_INSTANCE,
        // TypeId::Integer => &INTEGER_TYPE_INSTANCE,
        // TypeId::BigInt => &BIGINT_TYPE_INSTANCE,
        // TypeId::Decimal => &DECIMAL_TYPE_INSTANCE,
        // TypeId::Varchar => &VARLEN_TYPE_INSTANCE,
        // TypeId::Timestamp => &TIMESTAMP_TYPE_INSTANCE,
        // TypeId::Vector => &VECTOR_TYPE_INSTANCE,
        // TypeId::Invalid => &INVALID_TYPE_INSTANCE,
    }
}

// Ensure singleton instances are created for each type
static BOOLEAN_TYPE_INSTANCE: BooleanType = BooleanType;
static TINYINT_TYPE_INSTANCE: TinyIntType = TinyIntType;
static SMALLINT_TYPE_INSTANCE: SmallIntType = SmallIntType;
static INTEGER_TYPE_INSTANCE: IntegerType = IntegerType;
static BIGINT_TYPE_INSTANCE: BigIntType = BigIntType;
static DECIMAL_TYPE_INSTANCE: DecimalType = DecimalType;
static VARLEN_TYPE_INSTANCE: VarlenType = VarlenType;
static TIMESTAMP_TYPE_INSTANCE: TimestampType = TimestampType;
static VECTOR_TYPE_INSTANCE: VectorType = VectorType;
static INVALID_TYPE_INSTANCE: InvalidType = InvalidType;


pub struct InvalidType;
pub struct TinyIntType;
pub struct SmallIntType;
pub struct IntegerType;
pub struct BigIntType;
pub struct DecimalType;
pub struct VarlenType;
pub struct TimestampType;
pub struct VectorType;
//
// impl Type for InvalidType {
//     fn get_type_size(type_id: TypeId) -> u64 { 0 }
//     fn is_coercible_from(&self, type_id: TypeId) -> bool { false }
//     fn type_id_to_string(type_id: TypeId) -> String { "Invalid".to_string() }
//     fn get_min_value(type_id: TypeId) -> Value {
//
//     }
//     fn get_max_value(type_id: TypeId) -> Value { Value { /* fields */ } }
//     fn get_instance(type_id: TypeId) -> &'static dyn Type { &InvalidType }
//     fn get_type_id(&self) -> TypeId { TypeId::Invalid }
//     fn compare_equals(&self, left: &Value, right: &Value) -> CmpBool { CmpBool::CmpNull }
//     fn compare_not_equals(&self, left: &Value, right: &Value) -> CmpBool { CmpBool::CmpNull }
//     fn compare_less_than(&self, left: &Value, right: &Value) -> CmpBool { CmpBool::CmpNull }
//     fn compare_less_than_equals(&self, left: &Value, right: &Value) -> CmpBool { CmpBool::CmpNull }
//     fn compare_greater_than(&self, left: &Value, right: &Value) -> CmpBool { CmpBool::CmpNull }
//     fn compare_greater_than_equals(&self, left: &Value, right: &Value) -> CmpBool { CmpBool::CmpNull }
//     fn add(&self, left: &Value, right: &Value) -> Value { Value { /* fields */ } }
//     fn subtract(&self, left: &Value, right: &Value) -> Value { Value { /* fields */ } }
//     fn multiply(&self, left: &Value, right: &Value) -> Value { Value { /* fields */ } }
//     fn divide(&self, left: &Value, right: &Value) -> Value { Value { /* fields */ } }
//     fn modulo(&self, left: &Value, right: &Value) -> Value { Value { /* fields */ } }
//     fn min(&self, left: &Value, right: &Value) -> Value { Value { /* fields */ } }
//     fn max(&self, left: &Value, right: &Value) -> Value { Value { /* fields */ } }
//     fn sqrt(&self, val: &Value) -> Value { Value { /* fields */ } }
//     fn operate_null(&self, val: &Value, right: &Value) -> Value { Value { /* fields */ } }
//     fn is_zero(&self, val: &Value) -> bool { false }
//     fn is_inlined(&self, val: &Value) -> bool { false }
//     fn to_string(&self, val: &Value) -> String { "Invalid".to_string() }
//     fn serialize_to(&self, val: &Value, storage: &mut [u8]) {}
//     fn deserialize_from(&self, storage: &[u8]) -> Value { Value { /* fields */ } }
//     fn copy(&self, val: &Value) -> Value { Value { /* fields */ } }
//     fn cast_as(&self, val: &Value, type_id: TypeId) -> Value { Value { /* fields */ } }
//     fn get_data(&self, val: &Value) -> &[u8] { &[] }
//     fn get_storage_size(&self, val: &Value) -> u32 { 0 }
// }
