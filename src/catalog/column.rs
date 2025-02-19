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
            _ => panic!("Cannot get size of invalid type"),
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
}
