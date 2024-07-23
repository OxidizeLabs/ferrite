use column::Column;
use std::mem::size_of;

#[derive(Debug, Clone)]
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
                curr_offset += size_of::<u32>() as u32;
            }

            columns_processed.push(column);
        }

        Schema {
            tuple_is_inlined,
            columns: columns_processed,
            length: curr_offset,
            unlined_columns: uninlined_columns,
        }
    }

    pub fn copy_schema(from: &Schema, attrs: Vec<usize>) -> Schema {
        let columns: Vec<Column> = attrs.iter().map(|&i| from.columns[i].clone()).collect();
        Schema {
            columns,
            length: from.length,
            tuple_is_inlined: from.tuple_is_inlined,
            unlined_columns: from.unlined_columns.clone(), // Assuming uninlined_columns is not affected
        }
    }

    pub fn get_columns(&self) -> &Vec<Column> {
        &self.columns
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

// fn main() {
//     let col1 = Column::new("id".to_string(), true, 4);
//     let col2 = Column::new_varlen("name".to_string(), 100);
//     let col3 = Column::replicate("id_copy".to_string(), &col1);
//
//     println!("Column 1: {}", col1.to_string(false));
//     println!("Column 2: {}", col2.to_string(false));
//     println!("Column 3: {}", col3.to_string(false));
//
//     let columns = vec![col1, col2, col3];
//     let schema = Schema::new(columns);
//     println!("{}", schema.to_string(false));
//     println!("{}", schema.to_string(true));
//
//     let copied_schema = Schema::copy_schema(&schema, vec![0, 2]);
//     println!("Copied Schema: {}", copied_schema.to_string(false));
// }
