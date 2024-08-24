use std::fmt;
use std::fmt::{Display, Formatter};

use crate::types_db::type_id::TypeId;

#[derive(Clone, Debug)]
pub struct Column {
    column_name: String,
    column_type: TypeId,
    length: usize,
    column_offset: usize,
}

impl Column {
    pub fn type_size(type_id: TypeId, length: usize) -> u8 {
        match type_id {
            TypeId::Boolean | TypeId::TinyInt => 1,
            TypeId::SmallInt => 2,
            TypeId::Integer => 4,
            TypeId::BigInt | TypeId::Decimal | TypeId::Timestamp => 8,
            TypeId::VarChar => length as u8,
            // TypeId::Vector => (length * size_of::<f64>() as usize) as u8,
            _ => panic!("Cannot get size of invalid type"),
        }
    }
    pub fn new(column_name: String, column_type: TypeId) -> Self {
        assert_ne!(
            column_type,
            TypeId::VarChar,
            "Wrong constructor for VARCHAR type."
        );
        Column {
            column_name,
            column_type,
            length: Self::type_size(column_type, 0) as usize,
            column_offset: 0,
        }
    }
    pub fn new_varlen(column_name: String, column_type: TypeId, length: usize) -> Self {
        assert!(
            column_type == TypeId::VarChar || column_type == TypeId::Vector,
            "Wrong constructor for fixed-size type."
        );
        Column {
            column_name,
            column_type,
            length: Self::type_size(column_type, length) as usize,
            column_offset: 0,
        }
    }

    pub fn replicate(column_name: String, column: &Column) -> Self {
        Column {
            column_name,
            column_type: column.column_type,
            length: column.length,
            column_offset: column.column_offset,
        }
    }

    pub fn with_column_name(&self, column_name: String) -> Self {
        let mut c = self.clone();
        c.column_name = column_name;
        c
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
        self.column_offset = value
    }

    pub fn get_type(&self) -> TypeId {
        self.column_type
    }

    pub fn is_inlined(&self) -> bool {
        self.column_type != TypeId::VarChar
    }

    pub fn to_string(&self, simplified: bool) -> String {
        if simplified {
            format!("{}, {:?})", self.column_name, self.column_type)
        } else {
            format!(
                "Column(name: {}, type: {:?}, length: {}, offset: {})",
                self.column_name, self.column_type, self.length, self.column_offset
            )
        }
    }
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string(true))
    }
}

// fn main() {
//     let col1 = Column::new("id".to_string(), TypeId::Integer);
//     let col2 = Column::new_varlen("name".to_string(), TypeId::VarChar, 100);
//     let col3 = Column::replicate("id_copy".to_string(), &col1);
//
//     info!("Column 1: {}", col1);
//     info!("Column 2: {}", col2);
//     info!("Column 3: {}", col3);
// }
