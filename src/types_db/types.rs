use crate::types_db::bigint_type::BigIntType;
use crate::types_db::boolean_type::BooleanType;
use crate::types_db::decimal_type::DecimalType;
use crate::types_db::integer_type::IntegerType;
use crate::types_db::invalid_type::InvalidType;
use crate::types_db::smallint_type::SmallIntType;
use crate::types_db::timestamp_type::TimestampType;
use crate::types_db::tinyint_type::TinyIntType;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Val::Null;
use crate::types_db::value::{Val, Value};
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
    fn add(&self, other: &Value) -> Result<Value, String> {
        let type_id = self.get_type_id();
        if !type_id.get_value().is_numeric() {
            return Err(format!("Cannot perform addition on type {:?}", type_id));
        }
        // Implementation for specific types...
        unimplemented!()
    }
    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        unimplemented!()
    }
    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        unimplemented!()
    }
    fn divide(&self, _other: &Value) -> Result<Value, String> {
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
        unimplemented!()
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
    fn test_get_instance() {
        assert_eq!(get_instance(TypeId::Boolean).get_type_id(), TypeId::Boolean);
        assert_eq!(get_instance(TypeId::TinyInt).get_type_id(), TypeId::TinyInt);
        assert_eq!(
            get_instance(TypeId::SmallInt).get_type_id(),
            TypeId::SmallInt
        );
        assert_eq!(get_instance(TypeId::Integer).get_type_id(), TypeId::Integer);
        assert_eq!(get_instance(TypeId::BigInt).get_type_id(), TypeId::BigInt);
        assert_eq!(get_instance(TypeId::Decimal).get_type_id(), TypeId::Decimal);
        assert_eq!(
            get_instance(TypeId::Timestamp).get_type_id(),
            TypeId::Timestamp
        );
        assert_eq!(get_instance(TypeId::VarChar).get_type_id(), TypeId::VarChar);
        assert_eq!(get_instance(TypeId::Invalid).get_type_id(), TypeId::Invalid);
        assert_eq!(get_instance(TypeId::Vector).get_type_id(), TypeId::Vector);
    }

    #[test]
    fn test_type_size() {
        assert_eq!(get_type_size(TypeId::Boolean), 1);
        assert_eq!(get_type_size(TypeId::TinyInt), 1);
        assert_eq!(get_type_size(TypeId::SmallInt), 2);
        assert_eq!(get_type_size(TypeId::Integer), 4);
        assert_eq!(get_type_size(TypeId::BigInt), 8);
        assert_eq!(get_type_size(TypeId::Decimal), 8);
        assert_eq!(get_type_size(TypeId::Timestamp), 8);
        assert_eq!(get_type_size(TypeId::VarChar), 0);
        assert_eq!(get_type_size(TypeId::Invalid), 0);
        assert_eq!(
            get_type_size(TypeId::Vector),
            size_of::<Arc<Vec<Value>>>() as u64
        );
    }

    #[test]
    fn test_type_id_to_string() {
        assert_eq!(type_id_to_string(TypeId::Boolean), "Boolean");
        assert_eq!(type_id_to_string(TypeId::TinyInt), "TinyInt");
        assert_eq!(type_id_to_string(TypeId::SmallInt), "SmallInt");
        assert_eq!(type_id_to_string(TypeId::Integer), "Integer");
        assert_eq!(type_id_to_string(TypeId::BigInt), "BigInt");
        assert_eq!(type_id_to_string(TypeId::Decimal), "Decimal");
        assert_eq!(type_id_to_string(TypeId::VarChar), "VarChar");
        assert_eq!(type_id_to_string(TypeId::Timestamp), "Timestamp");
        assert_eq!(type_id_to_string(TypeId::Vector), "Vector");
        assert_eq!(type_id_to_string(TypeId::Invalid), "Invalid");
    }
}

#[cfg(test)]
mod type_behavior_tests {
    use super::*;

    #[test]
    fn test_min_values() {
        assert_eq!(
            BooleanType::get_min_value(TypeId::Boolean),
            Value::from(false)
        );
        assert_eq!(
            TinyIntType::get_min_value(TypeId::TinyInt),
            Value::from(i8::MIN)
        );
        assert_eq!(
            SmallIntType::get_min_value(TypeId::SmallInt),
            Value::from(i16::MIN)
        );
        assert_eq!(
            IntegerType::get_min_value(TypeId::Integer),
            Value::from(i32::MIN)
        );
        assert_eq!(
            BigIntType::get_min_value(TypeId::BigInt),
            Value::from(i64::MIN)
        );
        assert_eq!(
            DecimalType::get_min_value(TypeId::Decimal),
            Value::from(f64::MIN)
        );
        assert_eq!(
            TimestampType::get_min_value(TypeId::Timestamp),
            Value::from(0_u64)
        );
        assert_eq!(VarCharType::get_min_value(TypeId::VarChar), Value::from(""));
    }

    #[test]
    fn test_max_values() {
        assert_eq!(
            BooleanType::get_max_value(TypeId::Boolean),
            Value::from(true)
        );
        assert_eq!(
            TinyIntType::get_max_value(TypeId::TinyInt),
            Value::from(i8::MAX)
        );
        assert_eq!(
            SmallIntType::get_max_value(TypeId::SmallInt),
            Value::from(i16::MAX)
        );
        assert_eq!(
            IntegerType::get_max_value(TypeId::Integer),
            Value::from(i32::MAX)
        );
        assert_eq!(
            BigIntType::get_max_value(TypeId::BigInt),
            Value::from(i64::MAX)
        );
        assert_eq!(
            DecimalType::get_max_value(TypeId::Decimal),
            Value::from(f64::MAX)
        );
        assert_eq!(
            TimestampType::get_max_value(TypeId::Timestamp),
            Value::from(u64::MAX)
        );
        assert_eq!(VarCharType::get_max_value(TypeId::VarChar), Value::from(""));
    }

    #[test]
    fn test_invalid_type_behavior() {
        let invalid_type = InvalidType;
        assert_eq!(invalid_type.get_type_id(), TypeId::Invalid);
        assert_eq!(invalid_type.is_coercible_from(TypeId::Integer), false);
        assert_eq!(invalid_type.to_string(&Value::from(1)), "INVALID");
        assert_eq!(
            InvalidType::get_min_value(TypeId::Invalid),
            Value::from(Null)
        );
        assert_eq!(
            InvalidType::get_max_value(TypeId::Invalid),
            Value::from(Null)
        );
    }
}

#[cfg(test)]
mod type_coercion_tests {
    use super::*;

    #[test]
    fn test_numeric_coercions() {
        let int_type = get_instance(TypeId::Integer);
        let big_int_type = get_instance(TypeId::BigInt);
        let decimal_type = get_instance(TypeId::Decimal);
        let tiny_int_type = get_instance(TypeId::TinyInt);

        // All numeric types can be coerced from any other numeric type or varchar
        for numeric_type in &[int_type, big_int_type, decimal_type, tiny_int_type] {
            assert!(numeric_type.is_coercible_from(TypeId::TinyInt));
            assert!(numeric_type.is_coercible_from(TypeId::SmallInt));
            assert!(numeric_type.is_coercible_from(TypeId::Integer));
            assert!(numeric_type.is_coercible_from(TypeId::BigInt));
            assert!(numeric_type.is_coercible_from(TypeId::Decimal));
            assert!(numeric_type.is_coercible_from(TypeId::VarChar));

            // Numeric types cannot be coerced from boolean, timestamp, or vector
            assert!(!numeric_type.is_coercible_from(TypeId::Boolean));
            assert!(!numeric_type.is_coercible_from(TypeId::Timestamp));
            assert!(!numeric_type.is_coercible_from(TypeId::Vector));
        }
    }

    #[test]
    fn test_varchar_coercions() {
        let varchar_type = get_instance(TypeId::VarChar);

        // VarChar can be coerced from all basic types
        assert!(varchar_type.is_coercible_from(TypeId::Boolean));
        assert!(varchar_type.is_coercible_from(TypeId::TinyInt));
        assert!(varchar_type.is_coercible_from(TypeId::SmallInt));
        assert!(varchar_type.is_coercible_from(TypeId::Integer));
        assert!(varchar_type.is_coercible_from(TypeId::BigInt));
        assert!(varchar_type.is_coercible_from(TypeId::Decimal));
        assert!(varchar_type.is_coercible_from(TypeId::Timestamp));
        assert!(varchar_type.is_coercible_from(TypeId::VarChar));

        // VarChar cannot be coerced from vector or invalid types
        assert!(!varchar_type.is_coercible_from(TypeId::Vector));
        assert!(!varchar_type.is_coercible_from(TypeId::Invalid));
    }

    #[test]
    fn test_boolean_coercions() {
        let bool_type = get_instance(TypeId::Boolean);

        // Boolean accepts anything (returns true for all valid types)
        assert!(bool_type.is_coercible_from(TypeId::Boolean));
        assert!(bool_type.is_coercible_from(TypeId::TinyInt));
        assert!(bool_type.is_coercible_from(TypeId::SmallInt));
        assert!(bool_type.is_coercible_from(TypeId::Integer));
        assert!(bool_type.is_coercible_from(TypeId::BigInt));
        assert!(bool_type.is_coercible_from(TypeId::Decimal));
        assert!(bool_type.is_coercible_from(TypeId::Timestamp));
        assert!(bool_type.is_coercible_from(TypeId::VarChar));
        assert!(bool_type.is_coercible_from(TypeId::Vector));
    }

    #[test]
    fn test_timestamp_coercions() {
        let timestamp_type = get_instance(TypeId::Timestamp);

        // Timestamp only accepts varchar and itself
        assert!(timestamp_type.is_coercible_from(TypeId::Timestamp));
        assert!(timestamp_type.is_coercible_from(TypeId::VarChar));

        // Timestamp rejects other types
        assert!(!timestamp_type.is_coercible_from(TypeId::Boolean));
        assert!(!timestamp_type.is_coercible_from(TypeId::TinyInt));
        assert!(!timestamp_type.is_coercible_from(TypeId::SmallInt));
        assert!(!timestamp_type.is_coercible_from(TypeId::Integer));
        assert!(!timestamp_type.is_coercible_from(TypeId::BigInt));
        assert!(!timestamp_type.is_coercible_from(TypeId::Decimal));
        assert!(!timestamp_type.is_coercible_from(TypeId::Vector));
    }

    #[test]
    fn test_vector_coercions() {
        let vector_type = get_instance(TypeId::Vector);

        // Vector only accepts vector
        assert!(vector_type.is_coercible_from(TypeId::Vector));

        // Vector rejects all other types
        assert!(!vector_type.is_coercible_from(TypeId::Boolean));
        assert!(!vector_type.is_coercible_from(TypeId::TinyInt));
        assert!(!vector_type.is_coercible_from(TypeId::SmallInt));
        assert!(!vector_type.is_coercible_from(TypeId::Integer));
        assert!(!vector_type.is_coercible_from(TypeId::BigInt));
        assert!(!vector_type.is_coercible_from(TypeId::Decimal));
        assert!(!vector_type.is_coercible_from(TypeId::Timestamp));
        assert!(!vector_type.is_coercible_from(TypeId::VarChar));
        assert!(!vector_type.is_coercible_from(TypeId::Invalid));
    }

    #[test]
    fn test_invalid_type_coercions() {
        let invalid_type = get_instance(TypeId::Invalid);

        // Invalid type cannot be coerced from anything
        assert!(!invalid_type.is_coercible_from(TypeId::Invalid));
        assert!(!invalid_type.is_coercible_from(TypeId::Boolean));
        assert!(!invalid_type.is_coercible_from(TypeId::TinyInt));
        assert!(!invalid_type.is_coercible_from(TypeId::SmallInt));
        assert!(!invalid_type.is_coercible_from(TypeId::Integer));
        assert!(!invalid_type.is_coercible_from(TypeId::BigInt));
        assert!(!invalid_type.is_coercible_from(TypeId::Decimal));
        assert!(!invalid_type.is_coercible_from(TypeId::Timestamp));
        assert!(!invalid_type.is_coercible_from(TypeId::VarChar));
        assert!(!invalid_type.is_coercible_from(TypeId::Vector));
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    #[should_panic(expected = "Invalid type for min value")]
    fn test_invalid_min_value_type() {
        let _ = BooleanType::get_min_value(TypeId::Vector);
    }

    #[test]
    #[should_panic(expected = "Invalid type for max value")]
    fn test_invalid_max_value_type() {
        let _ = BooleanType::get_max_value(TypeId::Vector);
    }

    #[test]
    fn test_unimplemented_methods() {
        let invalid_type = InvalidType;
        let value = Value::from(1);

        // Test that unimplemented methods panic
        assert!(std::panic::catch_unwind(|| invalid_type.add(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.subtract(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.multiply(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.divide(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.modulo(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.min(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.max(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.sqrt(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.is_zero(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.is_inlined(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.copy(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.get_data(&value)).is_err());
        assert!(std::panic::catch_unwind(|| invalid_type.get_storage_size(&value)).is_err());
    }
}
