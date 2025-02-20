use crate::types_db::type_id::TypeId;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    column_name: String,
    column_type: TypeId,
    length: usize,
    column_offset: usize,
}

impl Column {
    fn type_size(type_id: TypeId, length: usize) -> usize {
        match type_id {
            TypeId::Boolean | TypeId::TinyInt => 1,
            TypeId::SmallInt => 2,
            TypeId::Integer => 4,
            TypeId::BigInt | TypeId::Decimal | TypeId::Timestamp => 8,
            TypeId::VarChar | TypeId::Char => length,
            TypeId::Vector => length * size_of::<f64>(),
            TypeId::Invalid => 0
        }
    }

    pub fn new(column_name: &str, column_type: TypeId) -> Self {
        Self {
            column_name: column_name.to_string(),
            column_type,
            length: Self::type_size(column_type, 0),
            column_offset: 0,
        }
    }

    pub fn new_varlen(column_name: &str, column_type: TypeId, length: usize) -> Self {
        assert!(
            matches!(column_type, TypeId::VarChar | TypeId::Vector | TypeId::Char),
            "Wrong constructor for fixed-size type."
        );
        Self {
            column_name: column_name.to_string(),
            column_type,
            length: Self::type_size(column_type, length),
            column_offset: 0,
        }
    }

    pub fn replicate(&self, new_name: &str) -> Self {
        Self {
            column_name: new_name.to_string(),
            ..*self
        }
    }

    pub fn with_name(&self, new_name: &str) -> Self {
        Self {
            column_name: new_name.to_string(),
            ..*self
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
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            write!(f, "Column(name: {}, type: {:?}, length: {}, offset: {})",
                   self.column_name, self.column_type, self.length, self.column_offset)
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
        assert_eq!(format!("{:#}", col), "Column(name: age, type: Integer, length: 4, offset: 0)");
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
            assert_eq!(col.get_storage_size(), expected_size, 
                "Wrong size for type {:?}", type_id);
            assert!(col.is_inlined(), 
                "Type {:?} should be inlined", type_id);
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
        assert_eq!(format!("{:#}", col2), 
            "Column(name: test, type: VarChar, length: 0, offset: 0)");
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
            TypeId::BigInt
        ];
        
        let col = Column::new("test", types[0]);
        let mut last_size = col.get_storage_size();
        
        // Verify size increases as we move to larger types
        for type_id in types.iter().skip(1) {
            let new_col = Column::new("test", *type_id);
            let new_size = new_col.get_storage_size();
            assert!(new_size >= last_size, 
                "Size should increase or stay same when moving to larger type");
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
}
