use column::Column;

pub struct Schema {
    columns: Vec<Column>,
    length: u32,
    tuple_is_inlined: bool,
    unlined_columns: Vec<u32>
}

impl Schema {
    pub fn copy_schema(from: Schema) -> Self {
        unimplemented!()
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

    pub fn get_unlined_column_count() -> u32 {
        unimplemented!()
    }

    pub fn get_column_count(&self) -> usize {
        self.columns.iter().count()
    }

    pub fn get_inlined_storage_size(&self) -> u32 {
        self.length
    }

    pub fn is_inlined() -> bool {
        unimplemented!()
    }
    pub fn to_string(&self, simplified: bool) -> String {
        unimplemented!()
    }
}