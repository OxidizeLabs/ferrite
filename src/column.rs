

pub struct Column {
    column_name: String,
    column_type: TypeId
}

impl Column {
    fn get_name(&self) -> String {
        self.column_name
    }
    fn get_type(&self) -> TypeId {
            self.column_type
    }

}

