use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::mem::size_of;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    column_name: String,
    column_type: TypeId,
    length: usize,
    column_offset: usize,
    is_primary_key: bool,
    is_not_null: bool,                         // NOT NULL constraint
    is_unique: bool,                           // UNIQUE constraint
    check_constraint: Option<String>,          // CHECK constraint expression
    default_value: Option<Value>,              // DEFAULT value
    foreign_key: Option<ForeignKeyConstraint>, // FOREIGN KEY constraint
    precision: Option<u8>,                     // For DECIMAL/NUMERIC types: total number of digits
    scale: Option<u8>, // For DECIMAL/NUMERIC types: number of digits after decimal point
}

/// Foreign key constraint information
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ForeignKeyConstraint {
    pub referenced_table: String,
    pub referenced_column: String,
    pub on_delete: Option<ReferentialAction>,
    pub on_update: Option<ReferentialAction>,
}

/// Referential actions for foreign key constraints
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

/// Builder for creating columns with specific parameters
#[derive(Debug, Clone)]
pub struct ColumnBuilder {
    column_name: String,
    column_type: TypeId,
    length: Option<usize>,
    is_primary_key: bool,
    is_not_null: bool,
    is_unique: bool,
    check_constraint: Option<String>,
    default_value: Option<Value>,
    foreign_key: Option<ForeignKeyConstraint>,
    precision: Option<u8>,
    scale: Option<u8>,
}

impl ColumnBuilder {
    /// Create a new column builder
    pub fn new(column_name: &str, column_type: TypeId) -> Self {
        Self {
            column_name: column_name.to_string(),
            column_type,
            length: None,
            is_primary_key: false,
            is_not_null: false,
            is_unique: false,
            check_constraint: None,
            default_value: None,
            foreign_key: None,
            precision: None,
            scale: None,
        }
    }

    /// Set the length for variable-length types (VARCHAR, CHAR, BINARY, etc.)
    pub fn with_length(mut self, length: usize) -> Self {
        self.length = Some(length);
        self
    }

    /// Set precision for numeric types (DECIMAL, NUMERIC, FLOAT)
    pub fn with_precision(mut self, precision: u8) -> Self {
        self.precision = Some(precision);
        self
    }

    /// Set precision and scale for decimal types (DECIMAL, NUMERIC)
    pub fn with_precision_and_scale(mut self, precision: u8, scale: u8) -> Self {
        self.precision = Some(precision);
        self.scale = Some(scale);
        self
    }

    /// Mark this column as a primary key
    pub fn as_primary_key(mut self) -> Self {
        self.is_primary_key = true;
        self
    }

    /// Mark this column as NOT NULL
    pub fn as_not_null(mut self) -> Self {
        self.is_not_null = true;
        self
    }

    /// Mark this column as UNIQUE
    pub fn as_unique(mut self) -> Self {
        self.is_unique = true;
        self
    }

    /// Set a CHECK constraint expression
    pub fn with_check_constraint(mut self, constraint: String) -> Self {
        self.check_constraint = Some(constraint);
        self
    }

    /// Set a default value
    pub fn with_default_value(mut self, value: Value) -> Self {
        self.default_value = Some(value);
        self
    }

    /// Set a foreign key constraint
    pub fn with_foreign_key(mut self, constraint: ForeignKeyConstraint) -> Self {
        self.foreign_key = Some(constraint);
        self
    }

    /// Build the column
    pub fn build(self) -> Column {
        let length = if let Some(len) = self.length {
            // For vectors, the length parameter is the dimension, so we need to calculate the actual storage size
            if self.column_type == TypeId::Vector {
                len * size_of::<f64>()
            } else {
                len
            }
        } else {
            Column::default_type_size(self.column_type)
        };

        Column {
            column_name: self.column_name,
            column_type: self.column_type,
            length,
            column_offset: 0,
            is_primary_key: self.is_primary_key,
            is_not_null: self.is_not_null,
            is_unique: self.is_unique,
            check_constraint: self.check_constraint,
            default_value: self.default_value,
            foreign_key: self.foreign_key,
            precision: self.precision,
            scale: self.scale,
        }
    }
}

impl Column {
    /// Get the default size for a type
    fn default_type_size(type_id: TypeId) -> usize {
        match type_id {
            TypeId::Boolean | TypeId::TinyInt => 1,
            TypeId::SmallInt => 2,
            TypeId::Integer => 4,
            TypeId::BigInt | TypeId::Decimal | TypeId::Timestamp => 8,
            TypeId::VarChar | TypeId::Char => 255, // Default length for strings
            TypeId::Vector => 1024,                // Default size for vectors
            TypeId::Invalid => 0,
            TypeId::Struct => 64, // Default size for structs
            TypeId::Float => 4,
            TypeId::Date => 4,
            TypeId::Time => 4,
            TypeId::Interval => 8,
            TypeId::Binary => 255, // Default length for binary
            TypeId::JSON => 1024,  // Default size for JSON
            TypeId::UUID => 16,
            TypeId::Array => size_of::<Vec<Value>>(),
            TypeId::Enum => 4,
            TypeId::Point => 16,
        }
    }

    fn type_size(type_id: TypeId, length: usize) -> usize {
        match type_id {
            TypeId::Boolean | TypeId::TinyInt => 1,
            TypeId::SmallInt => 2,
            TypeId::Integer => 4,
            TypeId::BigInt | TypeId::Decimal | TypeId::Timestamp => 8,
            TypeId::VarChar | TypeId::Char => length,
            TypeId::Vector => length * size_of::<f64>(),
            TypeId::Invalid => 0,
            TypeId::Struct => length,
            TypeId::Float => 4,
            TypeId::Date => 4,
            TypeId::Time => 4,
            TypeId::Interval => 8,
            TypeId::Binary => length,
            TypeId::JSON => length,
            TypeId::UUID => 16,
            TypeId::Array => size_of::<Vec<Value>>(),
            TypeId::Enum => 4,
            TypeId::Point => 16,
        }
    }

    /// Create a new column with default parameters
    pub fn new(column_name: &str, column_type: TypeId) -> Self {
        ColumnBuilder::new(column_name, column_type).build()
    }

    /// Create a column builder for more complex configurations
    pub fn builder(column_name: &str, column_type: TypeId) -> ColumnBuilder {
        ColumnBuilder::new(column_name, column_type)
    }

    /// Create a new variable-length column (VARCHAR, CHAR, BINARY, etc.)
    pub fn new_varlen(column_name: &str, column_type: TypeId, length: usize) -> Self {
        // Validate that this is actually a variable-length type
        if !matches!(
            column_type,
            TypeId::VarChar | TypeId::Char | TypeId::Binary | TypeId::Vector
        ) {
            panic!("Wrong constructor for fixed-size type.");
        }

        ColumnBuilder::new(column_name, column_type)
            .with_length(length)
            .build()
    }

    /// Create a new decimal column with precision and scale
    pub fn new_decimal(column_name: &str, precision: Option<u8>, scale: Option<u8>) -> Self {
        let mut builder = ColumnBuilder::new(column_name, TypeId::Decimal);
        if let Some(p) = precision {
            builder = builder.with_precision(p);
        }
        if let Some(s) = scale {
            builder = builder.with_precision_and_scale(precision.unwrap_or(10), s);
        }
        builder.build()
    }

    /// Create a new numeric column with precision and scale (alias for decimal)
    pub fn new_numeric(column_name: &str, precision: Option<u8>, scale: Option<u8>) -> Self {
        Self::new_decimal(column_name, precision, scale)
    }

    /// Create a new float column with precision
    pub fn new_float(column_name: &str, precision: Option<u8>) -> Self {
        let mut builder = ColumnBuilder::new(column_name, TypeId::Float);
        if let Some(p) = precision {
            builder = builder.with_precision(p);
        }
        builder.build()
    }

    /// Create a new primary key column
    pub fn new_primary_key(column_name: &str, column_type: TypeId) -> Self {
        ColumnBuilder::new(column_name, column_type)
            .as_primary_key()
            .build()
    }

    /// Create a column from SQL type information with all parameters
    pub fn from_sql_info(
        column_name: &str,
        column_type: TypeId,
        length: Option<usize>,
        precision: Option<u8>,
        scale: Option<u8>,
        is_primary_key: bool,
        is_not_null: bool,
        is_unique: bool,
        check_constraint: Option<String>,
        default_value: Option<Value>,
        foreign_key: Option<ForeignKeyConstraint>,
    ) -> Self {
        let mut builder = ColumnBuilder::new(column_name, column_type);

        if let Some(len) = length {
            builder = builder.with_length(len);
        }

        if let Some(p) = precision {
            if let Some(s) = scale {
                builder = builder.with_precision_and_scale(p, s);
            } else {
                builder = builder.with_precision(p);
            }
        }

        if is_primary_key {
            builder = builder.as_primary_key();
        }

        if is_not_null {
            builder = builder.as_not_null();
        }

        if is_unique {
            builder = builder.as_unique();
        }

        if let Some(constraint) = check_constraint {
            builder = builder.with_check_constraint(constraint);
        }

        if let Some(value) = default_value {
            builder = builder.with_default_value(value);
        }

        if let Some(fk) = foreign_key {
            builder = builder.with_foreign_key(fk);
        }

        builder.build()
    }

    pub fn replicate(&self, new_name: &str) -> Self {
        Self {
            column_name: new_name.to_string(),
            column_type: self.column_type,
            length: self.length,
            column_offset: self.column_offset,
            is_primary_key: self.is_primary_key,
            is_not_null: self.is_not_null,
            is_unique: self.is_unique,
            check_constraint: self.check_constraint.clone(),
            default_value: self.default_value.clone(),
            foreign_key: self.foreign_key.clone(),
            precision: self.precision,
            scale: self.scale,
        }
    }

    pub fn with_name(&self, new_name: &str) -> Self {
        Self {
            column_name: new_name.to_string(),
            column_type: self.column_type,
            length: self.length,
            column_offset: self.column_offset,
            is_primary_key: self.is_primary_key,
            is_not_null: self.is_not_null,
            is_unique: self.is_unique,
            check_constraint: self.check_constraint.clone(),
            default_value: self.default_value.clone(),
            foreign_key: self.foreign_key.clone(),
            precision: self.precision,
            scale: self.scale,
        }
    }

    pub fn get_name(&self) -> &str {
        &self.column_name
    }

    pub fn get_storage_size(&self) -> usize {
        self.length
    }

    pub fn get_offset(&self) -> usize {
        self.column_offset
    }

    pub fn set_offset(&mut self, value: usize) {
        self.column_offset = value;
    }

    pub fn get_type(&self) -> TypeId {
        self.column_type
    }

    pub fn is_inlined(&self) -> bool {
        self.column_type != TypeId::VarChar
    }

    pub fn set_name(&mut self, name: String) {
        self.column_name = name;
    }

    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }

    pub fn set_primary_key(&mut self, is_primary_key: bool) {
        self.is_primary_key = is_primary_key;
    }

    pub fn get_precision(&self) -> Option<u8> {
        self.precision
    }

    pub fn get_scale(&self) -> Option<u8> {
        self.scale
    }

    /// Set precision and scale for numeric types
    pub fn set_precision_scale(&mut self, precision: Option<u8>, scale: Option<u8>) {
        self.precision = precision;
        self.scale = scale;
    }

    /// Get the length parameter for variable-length types
    pub fn get_length(&self) -> usize {
        self.length
    }

    /// Set the length for variable-length types
    pub fn set_length(&mut self, length: usize) {
        self.length = length;
    }

    /// Check if this column type supports precision
    pub fn supports_precision(&self) -> bool {
        matches!(
            self.column_type,
            TypeId::Decimal | TypeId::Float | TypeId::Timestamp
        )
    }

    /// Check if this column type supports scale
    pub fn supports_scale(&self) -> bool {
        matches!(self.column_type, TypeId::Decimal)
    }

    /// Check if this column type supports length
    pub fn supports_length(&self) -> bool {
        matches!(
            self.column_type,
            TypeId::VarChar | TypeId::Char | TypeId::Binary | TypeId::Vector
        )
    }

    pub fn is_not_null(&self) -> bool {
        self.is_not_null
    }

    pub fn set_not_null(&mut self, is_not_null: bool) {
        self.is_not_null = is_not_null;
    }

    pub fn is_unique(&self) -> bool {
        self.is_unique
    }

    pub fn set_unique(&mut self, is_unique: bool) {
        self.is_unique = is_unique;
    }

    pub fn get_check_constraint(&self) -> &Option<String> {
        &self.check_constraint
    }

    pub fn set_check_constraint(&mut self, constraint: Option<String>) {
        self.check_constraint = constraint;
    }

    pub fn get_default_value(&self) -> &Option<Value> {
        &self.default_value
    }

    pub fn set_default_value(&mut self, value: Option<Value>) {
        self.default_value = value;
    }

    pub fn get_foreign_key(&self) -> &Option<ForeignKeyConstraint> {
        &self.foreign_key
    }

    pub fn set_foreign_key(&mut self, constraint: Option<ForeignKeyConstraint>) {
        self.foreign_key = constraint;
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(
                f,
                "Column(name: {}, type: {:?}, length: {}, offset: {})",
                self.column_name, self.column_type, self.length, self.column_offset
            )
        } else {
            write!(f, "{}({:?})", self.column_name, self.column_type)
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn column_creation() {
        let col1 = Column::new("id", TypeId::Integer);
        let col2 = Column::new_varlen("name", TypeId::VarChar, 100);
        let col3 = col1.replicate("id_copy");

        assert_eq!(col1.get_name(), "id");
        assert_eq!(col1.get_type(), TypeId::Integer);
        assert_eq!(col1.get_storage_size(), 4);

        assert_eq!(col2.get_name(), "name");
        assert_eq!(col2.get_type(), TypeId::VarChar);
        assert_eq!(col2.get_storage_size(), 100);

        assert_eq!(col3.get_name(), "id_copy");
        assert_eq!(col3.get_type(), TypeId::Integer);
        assert_eq!(col3.get_storage_size(), 4);
    }

    #[test]
    fn column_methods() {
        let mut col = Column::new("age", TypeId::SmallInt);

        assert_eq!(col.get_name(), "age");
        assert_eq!(col.get_type(), TypeId::SmallInt);
        assert_eq!(col.get_storage_size(), 2);
        assert_eq!(col.get_offset(), 0);
        assert!(col.is_inlined());

        col.set_offset(10);
        assert_eq!(col.get_offset(), 10);

        let renamed_col = col.with_name("new_age");
        assert_eq!(renamed_col.get_name(), "new_age");
        assert_eq!(renamed_col.get_type(), TypeId::SmallInt);
        assert_eq!(renamed_col.get_storage_size(), 2);
        assert_eq!(renamed_col.get_offset(), 10);
    }

    #[test]
    #[should_panic(expected = "Wrong constructor for fixed-size type.")]
    fn new_varlen_with_fixed_size_type() {
        Column::new_varlen("invalid", TypeId::Integer, 4);
    }

    #[test]
    fn test_display_formatting() {
        let col = Column::new("age", TypeId::Integer);
        assert_eq!(format!("{}", col), "age(Integer)");
        assert_eq!(
            format!("{:#}", col),
            "Column(name: age, type: Integer, length: 4, offset: 0)"
        );
    }

    #[test]
    fn test_vector_column() {
        use std::mem::size_of;
        let col = Column::new_varlen("embedding", TypeId::Vector, 5);
        assert_eq!(col.get_name(), "embedding");
        assert_eq!(col.get_type(), TypeId::Vector);
        assert_eq!(col.get_storage_size(), 5 * size_of::<f64>());
        assert!(col.is_inlined());
    }

    #[test]
    fn test_varchar_properties() {
        let col = Column::new_varlen("name", TypeId::VarChar, 50);
        assert_eq!(col.get_name(), "name");
        assert_eq!(col.get_type(), TypeId::VarChar);
        assert_eq!(col.get_storage_size(), 50);
        assert!(!col.is_inlined());
    }

    #[test]
    fn test_set_name() {
        let mut col = Column::new("old_name", TypeId::Integer);
        col.set_name("new_name".to_string());
        assert_eq!(col.get_name(), "new_name");
    }

    #[test]
    fn test_fixed_size_types() {
        let test_cases = vec![
            (TypeId::Boolean, 1),
            (TypeId::TinyInt, 1),
            (TypeId::SmallInt, 2),
            (TypeId::Integer, 4),
            (TypeId::BigInt, 8),
            (TypeId::Decimal, 8),
            (TypeId::Timestamp, 8),
        ];

        for (type_id, expected_size) in test_cases {
            let col = Column::new(&format!("col_{:?}", type_id), type_id);
            assert_eq!(
                col.get_storage_size(),
                expected_size,
                "Wrong size for type {:?}",
                type_id
            );
            assert!(col.is_inlined(), "Type {:?} should be inlined", type_id);
        }
    }

    #[test]
    fn test_char_type() {
        let col = Column::new_varlen("fixed_str", TypeId::Char, 10);
        assert_eq!(col.get_storage_size(), 10);
        assert!(col.is_inlined());
    }

    #[test]
    fn test_invalid_type() {
        let col = Column::new("invalid", TypeId::Invalid);
        assert_eq!(col.get_storage_size(), 0);
        assert!(col.is_inlined());
    }

    #[test]
    fn test_replicate_with_offset() {
        let mut col = Column::new("original", TypeId::Integer);
        col.set_offset(42);

        let replicated = col.replicate("copy");
        assert_eq!(replicated.get_name(), "copy");
        assert_eq!(replicated.get_type(), TypeId::Integer);
        assert_eq!(replicated.get_offset(), 42);
        assert_eq!(replicated.get_storage_size(), 4);
    }

    #[test]
    fn test_varchar_zero_length() {
        let col = Column::new_varlen("empty_str", TypeId::VarChar, 0);
        assert_eq!(col.get_storage_size(), 0);
        assert!(!col.is_inlined());
    }

    #[test]
    fn test_vector_large_dimension() {
        use std::mem::size_of;
        let dimension = 1024;
        let col = Column::new_varlen("large_vector", TypeId::Vector, dimension);
        assert_eq!(col.get_storage_size(), dimension * size_of::<f64>());
        assert!(col.is_inlined());
    }

    #[test]
    fn test_multiple_offset_updates() {
        let mut col = Column::new("test", TypeId::BigInt);
        assert_eq!(col.get_offset(), 0);

        col.set_offset(10);
        assert_eq!(col.get_offset(), 10);

        col.set_offset(20);
        assert_eq!(col.get_offset(), 20);

        col.set_offset(0);
        assert_eq!(col.get_offset(), 0);
    }

    #[test]
    fn test_chained_name_changes() {
        let col = Column::new("original", TypeId::SmallInt);
        let col2 = col.with_name("second");
        let col3 = col2.with_name("third");

        assert_eq!(col.get_name(), "original");
        assert_eq!(col2.get_name(), "second");
        assert_eq!(col3.get_name(), "third");

        // Verify other properties remain unchanged
        assert_eq!(col3.get_type(), TypeId::SmallInt);
        assert_eq!(col3.get_storage_size(), 2);
        assert_eq!(col3.get_offset(), 0);
    }

    #[test]
    fn test_column_name_empty_string() {
        let col = Column::new("", TypeId::Integer);
        assert_eq!(col.get_name(), "");
        assert_eq!(col.get_storage_size(), 4);
    }

    #[test]
    fn test_column_name_with_special_chars() {
        let special_chars = vec!["test#1", "hello@world", "first.second", "column_1"];
        for name in special_chars {
            let col = Column::new(name, TypeId::Integer);
            assert_eq!(col.get_name(), name);
        }
    }

    #[test]
    fn test_replicate_chain() {
        let original = Column::new("first", TypeId::Integer);
        let second = original.replicate("second");
        let third = second.replicate("third");
        let fourth = third.replicate("fourth");

        assert_eq!(fourth.get_name(), "fourth");
        assert_eq!(fourth.get_type(), TypeId::Integer);
        assert_eq!(fourth.get_storage_size(), 4);
    }

    #[test]
    fn test_varchar_max_length() {
        let col = Column::new_varlen("big_text", TypeId::VarChar, usize::MAX);
        assert_eq!(col.get_storage_size(), usize::MAX);
        assert!(!col.is_inlined());
    }

    #[test]
    fn test_display_special_cases() {
        let col1 = Column::new("", TypeId::Integer);
        let col2 = Column::new_varlen("test", TypeId::VarChar, 0);
        let col3 = Column::new("name.with.dots", TypeId::Boolean);

        assert_eq!(format!("{}", col1), "(Integer)");
        assert_eq!(
            format!("{:#}", col2),
            "Column(name: test, type: VarChar, length: 0, offset: 0)"
        );
        assert_eq!(format!("{}", col3), "name.with.dots(Boolean)");
    }

    #[test]
    fn test_offset_overflow() {
        let mut col = Column::new("test", TypeId::Integer);
        col.set_offset(usize::MAX);
        assert_eq!(col.get_offset(), usize::MAX);
    }

    #[test]
    fn test_multiple_mutations() {
        let mut col = Column::new("original", TypeId::Integer);

        // Test multiple mutations in sequence
        col.set_offset(5);
        col.set_name("new_name".to_string());
        col.set_offset(10);
        col.set_name("final_name".to_string());

        assert_eq!(col.get_name(), "final_name");
        assert_eq!(col.get_offset(), 10);
        assert_eq!(col.get_type(), TypeId::Integer);
    }

    #[test]
    fn test_comparison() {
        let col1 = Column::new("test", TypeId::Integer);
        let col2 = Column::new("test", TypeId::Integer);
        let col3 = Column::new("test", TypeId::SmallInt);
        let col4 = Column::new("other", TypeId::Integer);

        assert_eq!(col1, col2);
        assert_ne!(col1, col3);
        assert_ne!(col1, col4);
    }

    #[test]
    fn test_clone_behavior() {
        let original = Column::new("test", TypeId::Integer);
        let cloned = original.clone();

        assert_eq!(original, cloned);

        // Verify deep copy
        let mut cloned2 = original.clone();
        cloned2.set_name("modified".to_string());
        assert_ne!(original, cloned2);
    }

    #[test]
    fn test_unicode_column_names() {
        let unicode_names = vec!["测试", "テスト", "δοκιμή", "🚀", "test⚡️data"];
        for name in unicode_names {
            let col = Column::new(name, TypeId::Integer);
            assert_eq!(col.get_name(), name);
        }
    }

    #[test]
    fn test_whitespace_column_names() {
        let names = vec!["  leading", "trailing  ", "  both  ", "with spaces"];
        for name in names {
            let col = Column::new(name, TypeId::Integer);
            assert_eq!(col.get_name(), name);
        }
    }

    #[test]
    fn test_very_long_column_name() {
        let long_name = "a".repeat(1000);
        let col = Column::new(&long_name, TypeId::Integer);
        assert_eq!(col.get_name(), long_name);
    }

    #[test]
    fn test_column_type_transitions() {
        let types = vec![
            TypeId::Boolean,
            TypeId::TinyInt,
            TypeId::SmallInt,
            TypeId::Integer,
            TypeId::BigInt,
        ];

        let col = Column::new("test", types[0]);
        let mut last_size = col.get_storage_size();

        // Verify size increases as we move to larger types
        for type_id in types.iter().skip(1) {
            let new_col = Column::new("test", *type_id);
            let new_size = new_col.get_storage_size();
            assert!(
                new_size >= last_size,
                "Size should increase or stay same when moving to larger type"
            );
            last_size = new_size;
        }
    }

    #[test]
    fn test_vector_dimension_boundaries() {
        // Test small dimensions
        let small_dims = vec![1, 2, 3];
        for dim in small_dims {
            let col = Column::new_varlen("vec", TypeId::Vector, dim);
            assert_eq!(col.get_storage_size(), dim * std::mem::size_of::<f64>());
        }

        // Test power of 2 dimensions
        let pow2_dims = vec![2, 4, 8, 16, 32, 64];
        for dim in pow2_dims {
            let col = Column::new_varlen("vec", TypeId::Vector, dim);
            assert_eq!(col.get_storage_size(), dim * std::mem::size_of::<f64>());
        }
    }

    #[test]
    fn test_debug_format() {
        let col = Column::new("test", TypeId::Integer);
        let debug_str = format!("{:?}", col);
        assert!(debug_str.contains("test"));
        assert!(debug_str.contains("Integer"));
        assert!(debug_str.contains("length"));
        assert!(debug_str.contains("offset"));
    }

    #[test]
    fn test_serialization_consistency() {
        use serde_json;

        let original = Column::new("test", TypeId::Integer);
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: Column = serde_json::from_str(&serialized).unwrap();

        assert_eq!(original, deserialized);
        assert_eq!(original.get_name(), deserialized.get_name());
        assert_eq!(original.get_type(), deserialized.get_type());
        assert_eq!(original.get_storage_size(), deserialized.get_storage_size());
        assert_eq!(original.get_offset(), deserialized.get_offset());
    }

    #[test]
    fn test_consecutive_replications() {
        let original = Column::new("test", TypeId::Integer);
        let names = vec!["a", "b", "c", "d", "e"];

        let mut current = original.clone();
        for name in names {
            current = current.replicate(name);
            assert_eq!(current.get_name(), name);
            assert_eq!(current.get_type(), TypeId::Integer);
            assert_eq!(current.get_storage_size(), 4);
        }
    }

    #[test]
    fn test_primary_key_creation() {
        let pk_col = Column::new_primary_key("id", TypeId::Integer);
        assert!(pk_col.is_primary_key());
        assert_eq!(pk_col.get_name(), "id");
        assert_eq!(pk_col.get_type(), TypeId::Integer);
        assert_eq!(pk_col.get_storage_size(), 4);
    }

    #[test]
    fn test_primary_key_modification() {
        let mut col = Column::new("id", TypeId::Integer);
        assert!(!col.is_primary_key());

        col.set_primary_key(true);
        assert!(col.is_primary_key());

        col.set_primary_key(false);
        assert!(!col.is_primary_key());
    }

    #[test]
    fn test_primary_key_replication() {
        let pk_col = Column::new_primary_key("id", TypeId::Integer);
        let replicated = pk_col.replicate("id_copy");
        assert!(replicated.is_primary_key());
        assert_eq!(replicated.get_name(), "id_copy");
    }

    #[test]
    fn test_primary_key_with_name() {
        let pk_col = Column::new_primary_key("id", TypeId::Integer);
        let renamed = pk_col.with_name("new_id");
        assert!(renamed.is_primary_key());
        assert_eq!(renamed.get_name(), "new_id");
    }

    #[test]
    fn test_primary_key_serialization() {
        use serde_json;

        let original = Column::new_primary_key("id", TypeId::Integer);
        let serialized = serde_json::to_string(&original).unwrap();
        let deserialized: Column = serde_json::from_str(&serialized).unwrap();

        assert!(deserialized.is_primary_key());
        assert_eq!(original.get_name(), deserialized.get_name());
        assert_eq!(original.get_type(), deserialized.get_type());
    }

    #[test]
    fn test_primary_key_comparison() {
        let pk_col1 = Column::new_primary_key("id", TypeId::Integer);
        let pk_col2 = Column::new_primary_key("id", TypeId::Integer);
        let non_pk_col = Column::new("id", TypeId::Integer);

        assert_eq!(pk_col1, pk_col2);
        assert_ne!(pk_col1, non_pk_col);
    }

    #[test]
    fn test_primary_key_clone() {
        let original = Column::new_primary_key("id", TypeId::Integer);
        let cloned = original.clone();

        assert!(cloned.is_primary_key());
        assert_eq!(original.get_name(), cloned.get_name());
        assert_eq!(original.get_type(), cloned.get_type());
    }

    #[test]
    fn test_primary_key_display() {
        let pk_col = Column::new_primary_key("id", TypeId::Integer);
        let display_str = format!("{}", pk_col);
        assert_eq!(display_str, "id(Integer)");

        let debug_str = format!("{:#}", pk_col);
        assert!(debug_str.contains("id"));
        assert!(debug_str.contains("Integer"));
    }

    #[test]
    fn test_primary_key_with_different_types() {
        let types = vec![
            TypeId::Boolean,
            TypeId::TinyInt,
            TypeId::SmallInt,
            TypeId::Integer,
            TypeId::BigInt,
            TypeId::VarChar,
        ];

        for type_id in types {
            let pk_col = Column::new_primary_key("id", type_id);
            assert!(pk_col.is_primary_key());
            assert_eq!(pk_col.get_name(), "id");
            assert_eq!(pk_col.get_type(), type_id);
        }
    }

    #[test]
    fn test_primary_key_changes_preserve_other_properties() {
        let mut col = Column::new("test", TypeId::Integer);
        col.set_offset(100);
        col.set_primary_key(true);

        assert!(col.is_primary_key());
        assert_eq!(col.get_offset(), 100);
        assert_eq!(col.get_name(), "test");
        assert_eq!(col.get_type(), TypeId::Integer);
    }

    #[test]
    fn test_column_builder_basic() {
        let col = Column::builder("test_col", TypeId::VarChar)
            .with_length(100)
            .build();

        assert_eq!(col.get_name(), "test_col");
        assert_eq!(col.get_type(), TypeId::VarChar);
        assert_eq!(col.get_length(), 100);
        assert!(!col.is_primary_key());
    }

    #[test]
    fn test_column_builder_decimal_with_precision_scale() {
        let col = Column::builder("price", TypeId::Decimal)
            .with_precision_and_scale(10, 2)
            .build();

        assert_eq!(col.get_name(), "price");
        assert_eq!(col.get_type(), TypeId::Decimal);
        assert_eq!(col.get_precision(), Some(10));
        assert_eq!(col.get_scale(), Some(2));
    }

    #[test]
    fn test_column_builder_primary_key() {
        let col = Column::builder("id", TypeId::Integer)
            .as_primary_key()
            .build();

        assert_eq!(col.get_name(), "id");
        assert_eq!(col.get_type(), TypeId::Integer);
        assert!(col.is_primary_key());
    }

    #[test]
    fn test_column_builder_float_with_precision() {
        let col = Column::builder("measurement", TypeId::Float)
            .with_precision(7)
            .build();

        assert_eq!(col.get_name(), "measurement");
        assert_eq!(col.get_type(), TypeId::Float);
        assert_eq!(col.get_precision(), Some(7));
        assert_eq!(col.get_scale(), None);
    }

    #[test]
    fn test_from_sql_info_comprehensive() {
        let col = Column::from_sql_info(
            "complex_col",
            TypeId::Decimal,
            Some(50),
            Some(15),
            Some(5),
            true,
            true,
            true,
            Some("CHECK constraint".to_string()),
            Some(Value::from(123.45)),
            Some(ForeignKeyConstraint {
                referenced_table: "referenced_table".to_string(),
                referenced_column: "referenced_column".to_string(),
                on_delete: Some(ReferentialAction::Cascade),
                on_update: Some(ReferentialAction::SetNull),
            }),
        );

        assert_eq!(col.get_name(), "complex_col");
        assert_eq!(col.get_type(), TypeId::Decimal);
        assert_eq!(col.get_length(), 50);
        assert_eq!(col.get_precision(), Some(15));
        assert_eq!(col.get_scale(), Some(5));
        assert!(col.is_primary_key());
        assert!(col.is_not_null());
        assert!(col.is_unique());
        assert_eq!(
            col.get_check_constraint(),
            &Some("CHECK constraint".to_string())
        );
        assert_eq!(col.get_default_value(), &Some(Value::from(123.45)));
        assert_eq!(
            col.get_foreign_key(),
            &Some(ForeignKeyConstraint {
                referenced_table: "referenced_table".to_string(),
                referenced_column: "referenced_column".to_string(),
                on_delete: Some(ReferentialAction::Cascade),
                on_update: Some(ReferentialAction::SetNull),
            })
        );
    }

    #[test]
    fn test_supports_methods() {
        let decimal_col = Column::new("dec", TypeId::Decimal);
        assert!(decimal_col.supports_precision());
        assert!(decimal_col.supports_scale());
        assert!(!decimal_col.supports_length());

        let varchar_col = Column::new("str", TypeId::VarChar);
        assert!(!varchar_col.supports_precision());
        assert!(!varchar_col.supports_scale());
        assert!(varchar_col.supports_length());

        let float_col = Column::new("flt", TypeId::Float);
        assert!(float_col.supports_precision());
        assert!(!float_col.supports_scale());
        assert!(!float_col.supports_length());
    }

    #[test]
    fn test_default_type_sizes() {
        let varchar_col = Column::new("str", TypeId::VarChar);
        assert_eq!(varchar_col.get_length(), 255); // Default VARCHAR length

        let vector_col = Column::new("vec", TypeId::Vector);
        assert_eq!(vector_col.get_length(), 1024); // Default vector size

        let int_col = Column::new("num", TypeId::Integer);
        assert_eq!(int_col.get_length(), 4); // Integer size
    }

    #[test]
    fn test_set_length() {
        let mut col = Column::new("test", TypeId::VarChar);
        assert_eq!(col.get_length(), 255); // Default

        col.set_length(500);
        assert_eq!(col.get_length(), 500);
    }

    #[test]
    fn test_backward_compatibility() {
        // Test that old methods still work
        let decimal_col = Column::new_decimal("price", Some(10), Some(2));
        assert_eq!(decimal_col.get_precision(), Some(10));
        assert_eq!(decimal_col.get_scale(), Some(2));

        let numeric_col = Column::new_numeric("amount", Some(15), Some(3));
        assert_eq!(numeric_col.get_precision(), Some(15));
        assert_eq!(numeric_col.get_scale(), Some(3));

        let float_col = Column::new_float("measurement", Some(7));
        assert_eq!(float_col.get_precision(), Some(7));
        assert_eq!(float_col.get_scale(), None);

        let varlen_col = Column::new_varlen("name", TypeId::VarChar, 100);
        assert_eq!(varlen_col.get_length(), 100);

        let pk_col = Column::new_primary_key("id", TypeId::Integer);
        assert!(pk_col.is_primary_key());
    }
}
