use crate::types_db::array_type;
use crate::types_db::bigint_type;
use crate::types_db::binary_type;
use crate::types_db::boolean_type;
use crate::types_db::const_len_type;
use crate::types_db::date_type;
use crate::types_db::decimal_type;
use crate::types_db::enum_type;
use crate::types_db::float_type;
use crate::types_db::integer_type;
use crate::types_db::interval_type;
use crate::types_db::invalid_type;
use crate::types_db::json_type;
use crate::types_db::point_type;
use crate::types_db::smallint_type;
use crate::types_db::struct_type;
use crate::types_db::time_type;
use crate::types_db::timestamp_type;
use crate::types_db::tinyint_type;
use crate::types_db::type_id::TypeId;
use crate::types_db::uuid_type;
use crate::types_db::value::{Val, Value};
use crate::types_db::varlen_type;
use crate::types_db::vector_type;
use std::mem::size_of;
use std::sync::Arc;

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum CmpBool {
    CmpFalse = 0,
    CmpTrue = 1,
    CmpNull = 2,
}

pub trait Type {
    fn get_type_id(&self) -> TypeId;

    // **NEW: Static methods that receive actual values**
    // These replace the broken instance methods for comparisons
    fn compare_equals_static(_left_val: &Val, _right: &Value) -> CmpBool
    where
        Self: Sized,
    {
        // Default implementation - types can override this
        panic!("Static method compare_equals_static not implemented for this type")
    }

    fn compare_greater_than_static(_left_val: &Val, _right: &Value) -> CmpBool
    where
        Self: Sized,
    {
        // Default implementation - types can override this
        panic!("Static method compare_greater_than_static not implemented for this type")
    }

    fn compare_less_than_static(_left_val: &Val, _right: &Value) -> CmpBool
    where
        Self: Sized,
    {
        // Default implementation - types can override this
        panic!("Static method compare_less_than_static not implemented for this type")
    }

    // **EXISTING: Keep all existing methods for compatibility**
    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        match self.get_type_id() {
            TypeId::Invalid => false,
            TypeId::Boolean => true,
            TypeId::TinyInt
            | TypeId::SmallInt
            | TypeId::Integer
            | TypeId::BigInt
            | TypeId::Decimal
            | TypeId::Float => matches!(
                type_id,
                TypeId::TinyInt
                    | TypeId::SmallInt
                    | TypeId::Integer
                    | TypeId::BigInt
                    | TypeId::Decimal
                    | TypeId::Float
                    | TypeId::VarChar
            ),
            TypeId::Timestamp => type_id == TypeId::VarChar || type_id == TypeId::Timestamp,
            TypeId::Date => {
                type_id == TypeId::VarChar
                    || type_id == TypeId::Date
                    || type_id == TypeId::Timestamp
            },
            TypeId::Time => type_id == TypeId::VarChar || type_id == TypeId::Time,
            TypeId::Interval => type_id == TypeId::VarChar || type_id == TypeId::Interval,
            TypeId::VarChar => matches!(
                type_id,
                TypeId::Boolean
                    | TypeId::TinyInt
                    | TypeId::SmallInt
                    | TypeId::Integer
                    | TypeId::BigInt
                    | TypeId::Decimal
                    | TypeId::Float
                    | TypeId::Timestamp
                    | TypeId::Date
                    | TypeId::Time
                    | TypeId::Interval
                    | TypeId::VarChar
                    | TypeId::Char
                    | TypeId::UUID
                    | TypeId::JSON
            ),
            TypeId::Char => matches!(
                type_id,
                TypeId::Boolean
                    | TypeId::TinyInt
                    | TypeId::SmallInt
                    | TypeId::Integer
                    | TypeId::BigInt
                    | TypeId::Decimal
                    | TypeId::Float
                    | TypeId::Timestamp
                    | TypeId::Date
                    | TypeId::Time
                    | TypeId::Interval
                    | TypeId::VarChar
                    | TypeId::Char
            ),
            TypeId::Binary => type_id == TypeId::Binary,
            TypeId::JSON => type_id == TypeId::JSON || type_id == TypeId::VarChar,
            TypeId::UUID => type_id == TypeId::UUID || type_id == TypeId::VarChar,
            TypeId::Vector => type_id == TypeId::Vector,
            TypeId::Array => type_id == TypeId::Array,
            TypeId::Enum => {
                type_id == TypeId::Enum || type_id == TypeId::Integer || type_id == TypeId::VarChar
            },
            TypeId::Point => type_id == TypeId::Point,
            TypeId::Struct => type_id == TypeId::Struct,
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
            TypeId::Float => Value::from(f32::MIN),
            TypeId::Timestamp => Value::from(0_u64),
            TypeId::Date => Value::from(i32::MIN), // Minimum date value (days from epoch)
            TypeId::Time => Value::from(0),        // Minimum time value (seconds from midnight)
            TypeId::Interval => Value::from(i64::MIN), // Minimum interval (seconds)
            TypeId::VarChar => Value::from(""),
            TypeId::Char => Value::from(""),
            TypeId::Binary => Value::from(Vec::<u8>::new()),
            TypeId::JSON => Value::from("{}"),
            TypeId::UUID => Value::from("00000000-0000-0000-0000-000000000000"),
            TypeId::Vector => Value::from(Vec::<Value>::new()),
            TypeId::Array => Value::from(Vec::<Value>::new()),
            TypeId::Enum => Value::from(Val::Enum(0, String::new())),
            TypeId::Point => Value::from(Val::Point(f64::MIN, f64::MIN)),
            TypeId::Invalid => Value::from(Val::Null),
            TypeId::Struct => Value::from(Val::Null),
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
            TypeId::Float => Value::from(f32::MAX),
            TypeId::Timestamp => Value::from(u64::MAX),
            TypeId::Date => Value::from(i32::MAX), // Maximum date value (days from epoch)
            TypeId::Time => Value::from(86399),    // Maximum time value (23:59:59 in seconds)
            TypeId::Interval => Value::from(i64::MAX), // Maximum interval (seconds)
            TypeId::VarChar => Value::from(""),
            TypeId::Char => Value::from(""),
            TypeId::Binary => Value::from(Vec::<u8>::new()),
            TypeId::JSON => Value::from("{}"),
            TypeId::UUID => Value::from("ffffffff-ffff-ffff-ffff-ffffffffffff"),
            TypeId::Vector => Value::from(Vec::<Value>::new()),
            TypeId::Array => Value::from(Vec::<Value>::new()),
            TypeId::Enum => Value::from(Val::Enum(i32::MAX, String::new())),
            TypeId::Point => Value::from(Val::Point(f64::MAX, f64::MAX)),
            TypeId::Invalid => Value::from(Val::Null),
            TypeId::Struct => Value::from(Val::Null),
        }
    }

    fn compare_equals(&self, other: &Value) -> CmpBool;
    fn compare_not_equals(&self, other: &Value) -> CmpBool;
    fn compare_less_than(&self, other: &Value) -> CmpBool;
    fn compare_less_than_equals(&self, other: &Value) -> CmpBool;
    fn compare_greater_than(&self, other: &Value) -> CmpBool;
    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool;
    fn add(&self, other: &Value) -> Result<Value, String>;
    fn subtract(&self, other: &Value) -> Result<Value, String>;
    fn multiply(&self, other: &Value) -> Result<Value, String>;
    fn divide(&self, other: &Value) -> Result<Value, String>;
    fn modulo(&self, other: &Value) -> Value;
    fn min(&self, other: &Value) -> Value;
    fn max(&self, other: &Value) -> Value;
    fn to_string(&self, val: &Value) -> String;

    fn technical_display(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Boolean(b) => format!("BOOLEAN({})", b),
            Val::TinyInt(i) => format!("TINYINT({})", i),
            Val::SmallInt(i) => format!("SMALLINT({})", i),
            Val::Integer(i) => format!("INTEGER({})", i),
            Val::BigInt(i) => format!("BIGINT({})", i),
            Val::Decimal(d) => format!("DECIMAL({})", d),
            Val::Float(fl) => format!("FLOAT({})", fl),
            Val::Timestamp(t) => format!("TIMESTAMP({})", t),
            Val::Date(d) => format!("DATE({})", d),
            Val::Time(t) => format!("TIME({})", t),
            Val::Interval(i) => format!("INTERVAL({})", i),
            Val::VarLen(s) => format!("VARCHAR(\"{}\")", s),
            Val::ConstLen(s) => format!("CHAR(\"{}\")", s),
            Val::Binary(b) => format!("BINARY[{} bytes]", b.len()),
            Val::JSON(j) => format!("JSON({})", j),
            Val::UUID(u) => format!("UUID({})", u),
            Val::Vector(v) => {
                let type_name = type_id_to_string(self.get_type_id());
                format!("{}[{} items]", type_name, v.len())
            },
            Val::Array(a) => {
                let type_name = type_id_to_string(self.get_type_id());
                format!("{}[{} items]", type_name, a.len())
            },
            Val::Enum(id, name) => format!("ENUM({}: {})", id, name),
            Val::Point(x, y) => format!("POINT({}, {})", x, y),
            Val::Null => "NULL".to_string(),
            Val::Struct => "STRUCT".to_string(),
        }
    }

    fn sqrt(&self, _val: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn operate_null(&self, _left: &Value, _right: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn is_zero(&self, val: &Value) -> bool {
        match val.get_val() {
            Val::Integer(i) => *i == 0,
            Val::BigInt(i) => *i == 0,
            Val::SmallInt(i) => *i == 0,
            Val::TinyInt(i) => *i == 0,
            Val::Decimal(f) => *f == 0.0,
            _ => false,
        }
    }

    fn is_inlined(&self, _val: &Value) -> bool {
        true
    }

    fn copy(&self, val: &Value) -> Value {
        val.clone_optimized()
    }

    fn cast_as(&self, val: &Value, type_id: TypeId) -> Value {
        if !self.is_coercible_from(type_id) {
            return Value::new(Val::Null);
        }
        val.cast_to(self.get_type_id())
            .unwrap_or_else(|_| val.clone_optimized())
    }

    fn get_data(&self, _val: &Value) -> &[u8] {
        &[]
    }

    fn get_storage_size(&self, _val: &Value) -> u32 {
        get_type_size(self.get_type_id()) as u32
    }

    fn as_integer(&self) -> Result<i32, String> {
        Err("Cannot convert to integer".to_string())
    }

    fn as_bigint(&self) -> Result<i64, String> {
        Err("Cannot convert to bigint".to_string())
    }

    fn as_smallint(&self) -> Result<i16, String> {
        Err("Cannot convert to smallint".to_string())
    }

    fn as_tinyint(&self) -> Result<i8, String> {
        Err("Cannot convert to tinyint".to_string())
    }

    fn as_decimal(&self) -> Result<f64, String> {
        Err("Cannot convert to decimal".to_string())
    }

    fn as_bool(&self) -> Result<bool, String> {
        Err("Cannot convert to boolean".to_string())
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
        TypeId::Boolean => &boolean_type::BOOLEAN_TYPE_INSTANCE,
        TypeId::TinyInt => &tinyint_type::TINYINT_TYPE_INSTANCE,
        TypeId::SmallInt => &smallint_type::SMALLINT_TYPE_INSTANCE,
        TypeId::Integer => &integer_type::INTEGER_TYPE_INSTANCE,
        TypeId::BigInt => &bigint_type::BIGINT_TYPE_INSTANCE,
        TypeId::Decimal => &decimal_type::DECIMAL_TYPE_INSTANCE,
        TypeId::Float => &float_type::FLOAT_TYPE_INSTANCE,
        TypeId::VarChar => &varlen_type::VARCHAR_TYPE_INSTANCE,
        TypeId::Timestamp => &timestamp_type::TIMESTAMP_TYPE_INSTANCE,
        TypeId::Date => &date_type::DATE_TYPE_INSTANCE,
        TypeId::Time => &time_type::TIME_TYPE_INSTANCE,
        TypeId::Interval => &interval_type::INTERVAL_TYPE_INSTANCE,
        TypeId::Vector => &vector_type::VECTOR_TYPE_INSTANCE,
        TypeId::Binary => &binary_type::BINARY_TYPE_INSTANCE,
        TypeId::JSON => &json_type::JSON_TYPE_INSTANCE,
        TypeId::UUID => &uuid_type::UUID_TYPE_INSTANCE,
        TypeId::Array => &array_type::ARRAY_TYPE_INSTANCE,
        TypeId::Enum => &enum_type::ENUM_TYPE_INSTANCE,
        TypeId::Point => &point_type::POINT_TYPE_INSTANCE,
        TypeId::Invalid => &invalid_type::INVALID_TYPE_INSTANCE,
        TypeId::Char => &const_len_type::CHAR_TYPE_INSTANCE,
        TypeId::Struct => &struct_type::STRUCT_TYPE_INSTANCE,
    }
}

pub fn get_type_size(type_id: TypeId) -> u64 {
    match type_id {
        TypeId::Boolean | TypeId::TinyInt => 1,
        TypeId::SmallInt => 2,
        TypeId::Integer | TypeId::Float | TypeId::Date | TypeId::Time => 4,
        TypeId::BigInt | TypeId::Decimal | TypeId::Timestamp | TypeId::Interval => 8,
        TypeId::VarChar | TypeId::Invalid | TypeId::Binary | TypeId::JSON => 0,
        TypeId::UUID => 16,
        TypeId::Vector | TypeId::Array => size_of::<Arc<Vec<Value>>>() as u64,
        TypeId::Char => 0,
        TypeId::Enum => 4,   // ID size, the string is variable
        TypeId::Point => 16, // Two f64 values
        TypeId::Struct => size_of::<usize>() as u64, // Pointer size for struct
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
        TypeId::Float => "Float".to_string(),
        TypeId::VarChar => "VarChar".to_string(),
        TypeId::Timestamp => "Timestamp".to_string(),
        TypeId::Date => "Date".to_string(),
        TypeId::Time => "Time".to_string(),
        TypeId::Interval => "Interval".to_string(),
        TypeId::Vector => "Vector".to_string(),
        TypeId::Binary => "Binary".to_string(),
        TypeId::JSON => "JSON".to_string(),
        TypeId::UUID => "UUID".to_string(),
        TypeId::Array => "Array".to_string(),
        TypeId::Enum => "Enum".to_string(),
        TypeId::Point => "Point".to_string(),
        TypeId::Invalid => "Invalid".to_string(),
        TypeId::Char => "Char".to_string(),
        TypeId::Struct => "Struct".to_string(),
    }
}

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
    use crate::types_db::bigint_type::BigIntType;
    use crate::types_db::boolean_type::BooleanType;
    use crate::types_db::decimal_type::DecimalType;
    use crate::types_db::integer_type::IntegerType;
    use crate::types_db::invalid_type::InvalidType;
    use crate::types_db::smallint_type::SmallIntType;
    use crate::types_db::timestamp_type::TimestampType;
    use crate::types_db::tinyint_type::TinyIntType;
    use crate::types_db::varlen_type::VarCharType;

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
        assert!(!invalid_type.is_coercible_from(TypeId::Integer));
        assert_eq!(invalid_type.to_string(&Value::from(1)), "INVALID");
        assert_eq!(
            InvalidType::get_min_value(TypeId::Invalid),
            Value::from(Val::Null)
        );
        assert_eq!(
            InvalidType::get_max_value(TypeId::Invalid),
            Value::from(Val::Null)
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
    use crate::types_db::boolean_type::BooleanType;

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
}
