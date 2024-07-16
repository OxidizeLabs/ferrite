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
    fn get_type_id(&self) -> TypeId;
    fn is_coercible_from(&self, type_id: TypeId) -> bool {
        match self.get_type_id() {
            TypeId::Invalid => false,
            TypeId::Boolean => true,
            TypeId::TinyInt | TypeId::SmallInt | TypeId::Integer | TypeId::BigInt | TypeId::Decimal => {
                match type_id {
                    TypeId::TinyInt | TypeId::SmallInt | TypeId::Integer | TypeId::BigInt | TypeId::Decimal | TypeId::Varchar => true,
                    _ => false,
                }
            },
            TypeId::Timestamp => type_id == TypeId::Varchar || type_id == TypeId::Timestamp,
            TypeId::Varchar => {
                match type_id {
                    TypeId::Boolean | TypeId::TinyInt | TypeId::SmallInt | TypeId::Integer | TypeId::BigInt | TypeId::Decimal | TypeId::Timestamp | TypeId::Varchar => true,
                    _ => false,
                }
            },
            _ => type_id == self.get_type_id(),
        }
    }
    fn get_min_value(type_id: TypeId) -> Value where Self: Sized {
        match type_id {
            TypeId::Boolean => Value::from_boolean(TypeId::Boolean, 0),
            TypeId::TinyInt => Value::from_tinyint(TypeId::TinyInt, i8::MIN),
            TypeId::SmallInt => Value::from_smallint(TypeId::SmallInt, i16::MIN),
            TypeId::Integer => Value::from_integer(TypeId::Integer, i32::MIN),
            TypeId::BigInt => Value::from_bigint(TypeId::BigInt, i64::MIN),
            TypeId::Decimal => Value::from_decimal(TypeId::Decimal, f64::MIN),
            TypeId::Timestamp => Value::from_timestamp(TypeId::Timestamp, 0),
            TypeId::Varchar => Value::from_const_str(TypeId::Varchar, ""),
            _ => panic!()
        }
    }
    fn get_max_value(type_id: TypeId) -> Value where Self: Sized {
        match type_id {
            TypeId::Boolean => Value::from_boolean(TypeId::Boolean, 1),
            TypeId::TinyInt => Value::from_tinyint(TypeId::TinyInt, i8::MAX),
            TypeId::SmallInt => Value::from_smallint(TypeId::SmallInt, i16::MAX),
            TypeId::Integer => Value::from_integer(TypeId::Integer, i32::MAX),
            TypeId::BigInt => Value::from_bigint(TypeId::BigInt, i64::MAX),
            TypeId::Decimal => Value::from_decimal(TypeId::Decimal, f64::MAX),
            TypeId::Timestamp => Value::from_timestamp(TypeId::Timestamp, 0),
            TypeId::Varchar => Value::from_const_str(TypeId::Varchar, ""),
            _ => panic!()
        }
    }
    fn compare_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_not_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_less_than(&self, _left: &Value, right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_less_than_equals(&self, _left: &Value, right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_greater_than(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn compare_greater_than_equals(&self, _left: &Value, _right: &Value) -> CmpBool {
        unimplemented!()
    }
    fn add(&self, left: &Value, right: &Value) -> Value {
        unimplemented!()
    }
    fn subtract(&self, left: &Value, right: &Value) -> Value {
        unimplemented!()
    }
    fn multiply(&self, left: &Value, right: &Value) -> Value {
        unimplemented!()
    }
    fn divide(&self, left: &Value, right: &Value) -> Value {
        unimplemented!()
    }
    fn modulo(&self, left: &Value, right: &Value) -> Value {
        unimplemented!()
    }
    fn min(&self, left: &Value, right: &Value) -> Value {
        unimplemented!()
    }
    fn max(&self, left: &Value, right: &Value) -> Value {
        unimplemented!()
    }
    fn sqrt(&self, val: &Value) -> Value {
        unimplemented!()
    }
    fn operate_null(&self, val: &Value, right: &Value) -> Value {
        unimplemented!()
    }
    fn is_zero(&self, val: &Value) -> bool {
        unimplemented!()
    }
    fn is_inlined(&self, val: &Value) -> bool {
        unimplemented!()
    }
    fn to_string(&self, val: &Value) -> String {
        unimplemented!()
    }
    fn serialize_to(&self, val: &Value, storage: &mut [u8]) {
        unimplemented!()
    }
    fn deserialize_from(&self, storage: &[u8]) -> Value {
        unimplemented!()
    }
    fn copy(&self, val: &Value) -> Value {
        unimplemented!()
    }
    fn cast_as(&self, val: &Value, type_id: TypeId) -> Value {
        unimplemented!()
    }
    fn get_data(&self, val: &Value) -> &[u8] {
        unimplemented!()
    }
    fn get_storage_size(&self, val: &Value) -> u32 {
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

pub fn type_id_to_string(type_id: TypeId) -> String {
    match type_id {
        TypeId::Boolean => "Boolean".to_string(),
        TypeId::TinyInt => "TinyInt".to_string(),
        TypeId::SmallInt => "SmallInt".to_string(),
        TypeId::Integer => "Integer".to_string(),
        TypeId::BigInt => "BigInt".to_string(),
        TypeId::Decimal => "Decimal".to_string(),
        TypeId::Varchar => "Varchar".to_string(),
        TypeId::Timestamp => "Timestamp".to_string(),
        TypeId::Vector => "Vector".to_string(),
        TypeId::Invalid => "Invalid".to_string()
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
