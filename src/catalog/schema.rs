use crate::catalog::column::Column;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::mem::size_of;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    columns: Vec<Column>,
    length: u32,
    tuple_is_inlined: bool,
    unlined_columns: Vec<u32>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Schema {
        let mut curr_offset = 0;
        let mut tuple_is_inlined = true;
        let mut uninlined_columns: Vec<u32> = Vec::new();
        let mut columns_processed = Vec::new();

        for (index, mut column) in columns.into_iter().enumerate() {
            if !column.is_inlined() {
                tuple_is_inlined = false;
                uninlined_columns.push(index as u32);
            }
            column.set_offset(curr_offset);
            if column.is_inlined() {
                curr_offset += column.get_storage_size();
            } else {
                curr_offset += size_of::<usize>();
            }

            columns_processed.push(column);
        }

        Schema {
            tuple_is_inlined,
            columns: columns_processed,
            length: curr_offset as u32,
            unlined_columns: uninlined_columns,
        }
    }

    pub fn copy_schema(from: &Schema, attrs: &Vec<usize>) -> Schema {
        let columns: Vec<Column> = attrs.iter().map(|&i| from.columns[i].clone()).collect();
        Schema {
            columns,
            length: from.length,
            tuple_is_inlined: from.tuple_is_inlined,
            unlined_columns: from.unlined_columns.clone(),
        }
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
    }

    pub fn get_columns_mut(&mut self) -> &mut Vec<Column> {
        &mut self.columns
    }

    pub fn get_column(&self, column_index: usize) -> Option<&Column> {
        self.columns.get(column_index)
    }

    pub fn get_column_index(&self, column_name: &str) -> Option<usize> {
        for (index, column) in self.columns.iter().enumerate() {
            if column.get_name() == column_name {
                return Some(index);
            }
        }
        None
    }

    pub fn get_unlined_columns(&self) -> &Vec<u32> {
        &self.unlined_columns
    }

    pub fn get_unlined_column_count(&self) -> u32 {
        self.unlined_columns.len() as u32
    }

    pub fn get_column_count(&self) -> u32 {
        self.columns.len() as u32
    }

    pub fn get_inlined_storage_size(&self) -> u32 {
        self.length
    }

    pub fn is_inlined(&self) -> bool {
        self.tuple_is_inlined
    }

    pub fn to_string(&self, simplified: bool) -> String {
        if simplified {
            let column_strings: Vec<String> =
                self.columns.iter().map(|col| col.to_string(true)).collect();
            format!("({})", column_strings.join(", "))
        } else {
            let column_strings: Vec<String> = self
                .columns
                .iter()
                .map(|col| col.to_string(false))
                .collect();
            format!(
                "Schema[NumColumns: {}, IsInlined: {}, Length: {}] :: ({})",
                self.get_column_count(),
                self.tuple_is_inlined,
                self.length,
                column_strings.join(", ")
            )
        }
    }
}

impl PartialEq for Schema {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
            && self.length == other.length
            && self.tuple_is_inlined == other.tuple_is_inlined
            && self.unlined_columns == other.unlined_columns
    }
}

impl Display for Schema {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Schema {}", self.to_string(true))
    }
}

impl Default for Schema {
    fn default() -> Self {
        Schema {
            columns: Vec::new(),
            length: 0,
            tuple_is_inlined: true,
            unlined_columns: Vec::new(),
        }
    }
}

impl AsRef<Schema> for Schema {
    fn as_ref(&self) -> &Schema {
        self
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn schema_serialization() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ];
        let schema = Schema::new(columns);

        let serialized = bincode::serialize(&schema).unwrap();
        let deserialized: Schema = bincode::deserialize(&serialized).unwrap();

        assert_eq!(schema, deserialized);
    }

    #[test]
    fn schema_default() {
        let default_schema = Schema::default();
        assert_eq!(default_schema.get_column_count(), 0);
        assert_eq!(default_schema.get_inlined_storage_size(), 0);
        assert!(default_schema.is_inlined());
    }
}
