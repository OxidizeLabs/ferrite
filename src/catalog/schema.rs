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

    pub fn get_qualified_column_index(&self, column_name: &str) -> Option<usize> {
        // First try exact match (for already qualified names)
        if let Some(idx) = self.get_column_index(column_name) {
            return Some(idx);
        }
        
        // If the column name contains a dot (table.column format)
        if column_name.contains('.') {
            // Split the qualified name into parts
            let parts: Vec<&str> = column_name.split('.').collect();
            if parts.len() == 2 {
                let table_alias = parts[0];
                let col_name = parts[1];
                
                // Try to find a column with this qualified name pattern
                for (index, column) in self.columns.iter().enumerate() {
                    let col_name_parts: Vec<&str> = column.get_name().split('.').collect();
                    if col_name_parts.len() == 2 && col_name_parts[0] == table_alias && col_name_parts[1] == col_name {
                        return Some(index);
                    }
                }
                
                // If not found with qualification, try to find the unqualified column name
                // This helps when the schema has unqualified names but we're using qualified references
                for (index, column) in self.columns.iter().enumerate() {
                    if column.get_name() == col_name {
                        return Some(index);
                    }
                }
            }
        } else {
            // If it's an unqualified name, try to find it in any qualified column
            for (index, column) in self.columns.iter().enumerate() {
                let col_name_parts: Vec<&str> = column.get_name().split('.').collect();
                if col_name_parts.len() == 2 && col_name_parts[1] == column_name {
                    return Some(index);
                }
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

    pub fn merge(left: &Schema, right: &Schema) -> Schema {
        let mut merged_columns = left.get_columns().clone();
        merged_columns.extend(right.get_columns().iter().cloned());
        Schema::new(merged_columns)
    }

    pub fn merge_with_aliases(left: &Schema, right: &Schema, left_alias: Option<&str>, right_alias: Option<&str>) -> Schema {
        let mut merged_columns = Vec::new();
        
        // Add left columns with alias if provided
        for col in left.get_columns() {
            if let Some(alias) = left_alias {
                // Only add alias if the column name doesn't already have one
                if !col.get_name().contains('.') {
                    let mut new_col = col.clone();
                    new_col.set_name(format!("{}.{}", alias, col.get_name()));
                    merged_columns.push(new_col);
                } else {
                    // If the column already has an alias but it's different, update it
                    let parts: Vec<&str> = col.get_name().split('.').collect();
                    if parts.len() == 2 && parts[0] != alias {
                        let mut new_col = col.clone();
                        new_col.set_name(format!("{}.{}", alias, parts[1]));
                        merged_columns.push(new_col);
                    } else {
                        merged_columns.push(col.clone());
                    }
                }
            } else {
                merged_columns.push(col.clone());
            }
        }
        
        // Add right columns with alias if provided
        for col in right.get_columns() {
            if let Some(alias) = right_alias {
                // Only add alias if the column name doesn't already have one
                if !col.get_name().contains('.') {
                    let mut new_col = col.clone();
                    new_col.set_name(format!("{}.{}", alias, col.get_name()));
                    merged_columns.push(new_col);
                } else {
                    // If the column already has an alias but it's different, update it
                    let parts: Vec<&str> = col.get_name().split('.').collect();
                    if parts.len() == 2 && parts[0] != alias {
                        let mut new_col = col.clone();
                        new_col.set_name(format!("{}.{}", alias, parts[1]));
                        merged_columns.push(new_col);
                    } else {
                        merged_columns.push(col.clone());
                    }
                }
            } else {
                merged_columns.push(col.clone());
            }
        }
        
        Schema::new(merged_columns)
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
        if f.alternate() {
            // Detailed format
            let column_strings: Vec<String> = self
                .columns
                .iter()
                .map(|col| format!("{:#}", col))
                .collect();

            write!(
                f,
                "Schema[NumColumns: {}, IsInlined: {}, Length: {}] :: ({})",
                self.get_column_count(),
                self.tuple_is_inlined,
                self.length,
                column_strings.join(", ")
            )
        } else {
            // Basic format
            let column_strings: Vec<String> = self
                .columns
                .iter()
                .map(|col| format!("{}", col))
                .collect();

            write!(f, "Schema ({})", column_strings.join(", "))
        }
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

    #[test]
    fn test_schema_merge() {
        let left_columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ];
        let right_columns = vec![
            Column::new("age", TypeId::Integer),
            Column::new("email", TypeId::VarChar),
        ];

        let left_schema = Schema::new(left_columns);
        let right_schema = Schema::new(right_columns);
        let merged_schema = Schema::merge(&left_schema, &right_schema);

        assert_eq!(merged_schema.get_column_count(), 4);
        assert_eq!(merged_schema.get_column_index("id"), Some(0));
        assert_eq!(merged_schema.get_column_index("email"), Some(3));
    }

    #[test]
    fn test_schema_column_operations() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ];
        let schema = Schema::new(columns);

        // Test column retrieval
        assert_eq!(schema.get_column(0).unwrap().get_name(), "id");
        assert_eq!(schema.get_column(1).unwrap().get_name(), "name");
        assert!(schema.get_column(5).is_none());

        // Test column index lookup
        assert_eq!(schema.get_column_index("age"), Some(2));
        assert_eq!(schema.get_column_index("nonexistent"), None);

        // Test inlined status
        assert!(!schema.is_inlined()); // Because VarChar is not inlined
        assert_eq!(schema.get_unlined_column_count(), 1); // One VarChar column
    }

    #[test]
    fn test_schema_copy() {
        let original_columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ];
        let original_schema = Schema::new(original_columns);

        // Copy only id and age columns
        let copied_schema = Schema::copy_schema(&original_schema, &vec![0, 2]);

        assert_eq!(copied_schema.get_column_count(), 2);
        assert_eq!(copied_schema.get_column(0).unwrap().get_name(), "id");
        assert_eq!(copied_schema.get_column(1).unwrap().get_name(), "age");
    }

    #[test]
    fn test_schema_display() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ];
        let schema = Schema::new(columns);

        // Test basic format
        let basic_format = format!("{}", schema);
        assert!(basic_format.contains("id"));
        assert!(basic_format.contains("name"));

        // Test detailed format
        let detailed_format = format!("{:#}", schema);
        assert!(detailed_format.contains("NumColumns: 2"));
        assert!(detailed_format.contains("IsInlined: false"));
    }

    #[test]
    fn test_schema_storage_layout() {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("flag", TypeId::Boolean),
            Column::new("name", TypeId::VarChar),
        ];
        let schema = Schema::new(columns);

        // Check storage offsets are calculated correctly
        let columns = schema.get_columns();
        assert_eq!(columns[0].get_offset(), 0); // id starts at 0
        assert!(columns[1].get_offset() > columns[0].get_offset()); // flag comes after id
        assert!(columns[2].get_offset() > columns[1].get_offset()); // name comes after flag

        // Check uninlined columns
        let uninlined = schema.get_unlined_columns();
        assert_eq!(uninlined.len(), 1); // Only VarChar is uninlined
        assert_eq!(uninlined[0], 2); // VarChar is at index 2
    }
}
