pub const DBL_LOWEST: f64 = std::f64::MIN;
pub const FLT_LOWEST: f32 = std::f32::MIN;

pub const DB_INT8_MIN: i8 = i8::MIN + 1;
pub const DB_INT16_MIN: i16 = i16::MIN + 1;
pub const DB_INT32_MIN: i32 = i32::MIN + 1;
pub const DB_INT64_MIN: i64 = i64::MIN + 1;
pub const DB_DECIMAL_MIN: f64 = FLT_LOWEST as f64;
pub const DB_TIMESTAMP_MIN: u64 = 0;
pub const DB_DATE_MIN: u32 = 0;
pub const DB_BOOLEAN_MIN: i8 = 0;

pub const DB_INT8_MAX: i8 = i8::MAX;
pub const DB_INT16_MAX: i16 = i16::MAX;
pub const DB_INT32_MAX: i32 = i32::MAX;
pub const DB_INT64_MAX: i64 = i64::MAX;
pub const DB_UINT64_MAX: u64 = u64::MAX - 1;
pub const DB_DECIMAL_MAX: f64 = std::f64::MAX;
pub const DB_TIMESTAMP_MAX: u64 = 11231999986399999999;
pub const DB_DATE_MAX: u64 = i32::MAX as u64;
pub const DB_BOOLEAN_MAX: i8 = 1;

pub const DB_VALUE_NULL: u32 = u32::MAX;
pub const DB_INT8_NULL: i8 = i8::MIN;
pub const DB_INT16_NULL: i16 = i16::MIN;
pub const DB_INT32_NULL: i32 = i32::MIN;
pub const DB_INT64_NULL: i64 = i64::MIN;
pub const DB_DATE_NULL: u64 = 0;
pub const DB_TIMESTAMP_NULL: u64 = u64::MAX;
pub const DB_DECIMAL_NULL: f64 = DBL_LOWEST;
pub const DB_BOOLEAN_NULL: i8 = i8::MIN;

pub const DB_VARCHAR_MAX_LEN: u32 = u32::MAX;

// Use to make TEXT type as the alias of VARCHAR(TEXT_MAX_LENGTH)
pub const DB_TEXT_MAX_LEN: u32 = 1_000_000_000;

// Objects (i.e., VARCHAR) with length prefix of -1 are NULL
pub const OBJECTLENGTH_NULL: i32 = -1;

