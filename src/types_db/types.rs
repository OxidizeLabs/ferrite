use crate::types_db::bigint_type::BigIntType;
use crate::types_db::boolean_type::BooleanType;
use crate::types_db::decimal_type::DecimalType;
use crate::types_db::integer_type::IntegerType;
use crate::types_db::smallint_type::SmallIntType;
use crate::types_db::timestamp_type::TimestampType;
use crate::types_db::tinyint_type::TinyIntType;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Val::Null;
use crate::types_db::value::Value;
use crate::types_db::varlen_type::VarCharType;
use crate::types_db::vector_type::VectorType;
use std::mem::size_of;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq)]
pub enum CmpBool {
    CmpFalse = 0,
    CmpTrue = 1,
    CmpNull = 2,
}

pub struct InvalidType;

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
            | TypeId::Decimal => matches!(
                type_id,
                TypeId::TinyInt
                    | TypeId::SmallInt
                    | TypeId::Integer
                    | TypeId::BigInt
                    | TypeId::Decimal
                    | TypeId::VarChar
            ),
            TypeId::Timestamp => type_id == TypeId::VarChar || type_id == TypeId::Timestamp,
            TypeId::VarChar => matches!(
                type_id,
                TypeId::Boolean
                    | TypeId::TinyInt
                    | TypeId::SmallInt
                    | TypeId::Integer
                    | TypeId::BigInt
                    | TypeId::Decimal
                    | TypeId::Timestamp
                    | TypeId::VarChar
            ),
            TypeId::Vector => matches!(type_id, TypeId::Vector),
        }
    }
    fn get_min_value(type_id: TypeId) -> Value
    where
        Self: Sized,
    {
        match type_id {
            TypeId::Boolean => Value::from(false),
            TypeId::TinyInt => Value::from(i8::MIN),
            TypeId::SmallInt => Value::from(i16::MIN),
            TypeId::Integer => Value::from(i32::MIN),
            TypeId::BigInt => Value::from(i64::MIN),
            TypeId::Decimal => Value::from(f64::MIN),
            TypeId::Timestamp => Value::from(0_u64),
            TypeId::VarChar => Value::from(""),
            TypeId::Invalid => Value::from(Null),
            _ => panic!("Invalid type for min value"),
        }
    }
    fn get_max_value(type_id: TypeId) -> Value
    where
        Self: Sized,
    {
        match type_id {
            TypeId::Boolean => Value::from(true),
            TypeId::TinyInt => Value::from(i8::MAX),
            TypeId::SmallInt => Value::from(i16::MAX),
            TypeId::Integer => Value::from(i32::MAX),
            TypeId::BigInt => Value::from(i64::MAX),
            TypeId::Decimal => Value::from(f64::MAX),
            TypeId::Timestamp => Value::from(u64::MAX),
            TypeId::VarChar => Value::from(""),
            TypeId::Invalid => Value::from(Null),
            _ => panic!("Invalid type for max value"),
        }
    }
    fn compare_equals(&self, _other: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_not_equals(&self, _other: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_less_than(&self, _other: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_less_than_equals(&self, _other: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_greater_than(&self, _other: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_greater_than_equals(&self, _other: &Value) -> CmpBool {
        unimplemented!()
    }
    fn add(&self, _other: &Value) -> Value {
        unimplemented!()
    }
    fn subtract(&self, _other: &Value) -> Value {
        unimplemented!()
    }
    fn multiply(&self, _other: &Value) -> Value {
        unimplemented!()
    }
    fn divide(&self, _other: &Value) -> Value {
        unimplemented!()
    }
    fn modulo(&self, _other: &Value) -> Value {
        unimplemented!()
    }
    fn min(&self, _other: &Value) -> Value {
        unimplemented!()
    }
    fn max(&self, _other: &Value) -> Value {
        unimplemented!()
    }
    fn sqrt(&self, _val: &Value) -> Value {
        unimplemented!()
    }
    fn operate_null(&self, _left: &Value, _right: &Value) -> Value {
        unimplemented!()
    }
    fn is_zero(&self, _val: &Value) -> bool {
        unimplemented!()
    }
    fn is_inlined(&self, _val: &Value) -> bool {
        unimplemented!()
    }
    fn to_string(&self, _val: &Value) -> String {
        "INVALID".to_string()
    }
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

impl Type for InvalidType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Invalid
    }
}

impl From<bool> for CmpBool {
    fn from(b: bool) -> Self {
        if b {
            CmpBool::CmpTrue
        } else {
            CmpBool::CmpFalse
        }
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
        TypeId::VarChar | TypeId::Invalid => 0,
        TypeId::Vector => size_of::<Arc<Vec<Value>>>() as u64,
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

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn test_cmp_bool_from_bool() {
        assert_eq!(CmpBool::from(true), CmpBool::CmpTrue);
        assert_eq!(CmpBool::from(false), CmpBool::CmpFalse);
    }

    #[test]
    fn test_get_min_value() {
        assert_eq!(
            BooleanType::get_min_value(TypeId::Boolean),
            Value::from(false)
        );
        assert_eq!(
            TinyIntType::get_min_value(TypeId::TinyInt),
            Value::from(i8::MIN)
        );
        assert_eq!(
            IntegerType::get_min_value(TypeId::Integer),
            Value::from(i32::MIN)
        );
    }

    #[test]
    fn test_get_max_value() {
        assert_eq!(
            BooleanType::get_max_value(TypeId::Boolean),
            Value::from(true)
        );
        assert_eq!(
            TinyIntType::get_max_value(TypeId::TinyInt),
            Value::from(i8::MAX)
        );
        assert_eq!(
            IntegerType::get_max_value(TypeId::Integer),
            Value::from(i32::MAX)
        );
    }

    #[test]
    fn test_is_coercible_from() {
        let int_type = get_instance(TypeId::Integer);
        assert_eq!(int_type.is_coercible_from(TypeId::SmallInt), true);
        assert_eq!(int_type.is_coercible_from(TypeId::TinyInt), true);
        assert_eq!(int_type.is_coercible_from(TypeId::Boolean), false);
    }

    #[test]
    fn test_get_type_size() {
        assert_eq!(get_type_size(TypeId::Boolean), 1);
        assert_eq!(get_type_size(TypeId::Integer), 4);
        assert_eq!(get_type_size(TypeId::BigInt), 8);
    }

    #[test]
    fn test_type_id_to_string() {
        assert_eq!(type_id_to_string(TypeId::Boolean), "Boolean");
        assert_eq!(type_id_to_string(TypeId::Integer), "Integer");
        assert_eq!(type_id_to_string(TypeId::VarChar), "VarChar");
    }

    #[test]
    fn test_invalid_type() {
        let invalid_type = InvalidType;
        assert_eq!(invalid_type.get_type_id(), TypeId::Invalid);
        assert_eq!(invalid_type.is_coercible_from(TypeId::Integer), false);
        assert_eq!(invalid_type.to_string(&Value::from(1)), "INVALID");
    }
}
