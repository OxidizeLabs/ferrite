use std::sync::Arc;

use crate::catalogue::schema::Schema;
use crate::common::rid::RID;
use crate::concurrency::transaction::Transaction;
use crate::storage::table::tuple::Tuple;

/// Holds metadata of an index object.
pub struct IndexMetadata {
    name: String,
    table_name: String,
    key_attrs: Vec<usize>,
    key_schema: Arc<Schema>,
    is_primary_key: bool,
}

impl IndexMetadata {
    /// Constructs a new `IndexMetadata` instance.
    ///
    /// # Parameters
    /// - `index_name`: The name of the index.
    /// - `table_name`: The name of the table on which the index is created.
    /// - `tuple_schema`: The schema of the indexed key.
    /// - `key_attrs`: The mapping from indexed columns to base table columns.
    /// - `is_primary_key`: Whether this index is a primary key.
    pub fn new(
        index_name: String,
        table_name: String,
        tuple_schema: &Schema,
        key_attrs: Vec<usize>,
        is_primary_key: bool,
    ) -> Self {
        let key_schema = Arc::new(Schema::copy_schema(tuple_schema, &key_attrs));
        Self {
            name: index_name,
            table_name,
            key_attrs,
            key_schema,
            is_primary_key,
        }
    }

    /// Returns the name of the index.
    pub fn get_name(&self) -> &str {
        &self.name
    }

    /// Returns the name of the table on which the index is created.
    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    /// Returns a schema object pointer that represents the indexed key.
    pub fn get_key_schema(&self) -> &Schema {
        &self.key_schema
    }

    /// Returns the number of columns inside the index key.
    pub fn get_index_column_count(&self) -> u32 {
        self.key_attrs.len() as u32
    }

    /// Returns the mapping relation between indexed columns and base table columns.
    pub fn get_key_attrs(&self) -> &Vec<usize> {
        &self.key_attrs
    }

    /// Returns whether this index is a primary key.
    pub fn is_primary_key(&self) -> bool {
        self.is_primary_key
    }

    /// Returns a string representation for debugging.
    pub fn to_string(&self) -> String {
        format!(
            "IndexMetadata[Name = {}, Type = B+Tree, Table name = {}] :: {}",
            self.name,
            self.table_name,
            self.key_schema.to_string(false)
        )
    }
}

/// Base class for derived indices of different types.
pub trait Index {
    /// Constructs a new `Index` instance.
    ///
    /// # Parameters
    /// - `metadata`: An owning pointer to the index metadata.
    fn new(metadata: Box<IndexMetadata>) -> Self
    where
        Self: Sized;

    /// Returns a non-owning pointer to the metadata object associated with the index.
    fn get_metadata(&self) -> &IndexMetadata;

    /// Returns the number of indexed columns.
    fn get_index_column_count(&self) -> u32 {
        self.get_metadata().get_index_column_count()
    }

    /// Returns the index name.
    fn get_name(&self) -> &str {
        self.get_metadata().get_name()
    }

    /// Returns the index key schema.
    fn get_key_schema(&self) -> &Schema {
        self.get_metadata().get_key_schema()
    }

    /// Returns the index key attributes.
    fn get_key_attrs(&self) -> &Vec<usize> {
        self.get_metadata().get_key_attrs()
    }

    /// Returns a string representation for debugging.
    fn to_string(&self) -> String {
        format!(
            "INDEX: ({}){}",
            self.get_name(),
            self.get_metadata().to_string()
        )
    }

    /// Inserts an entry into the index.
    ///
    /// # Parameters
    /// - `key`: The index key.
    /// - `rid`: The RID associated with the key.
    /// - `transaction`: The transaction context.
    ///
    /// # Returns
    /// Whether insertion is successful.
    fn insert_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) -> bool {
        unimplemented!()
    }

    /// Deletes an index entry by key.
    ///
    /// # Parameters
    /// - `key`: The index key.
    /// - `rid`: The RID associated with the key (unused).
    /// - `transaction`: The transaction context.
    fn delete_entry(&self, key: &Tuple, rid: RID, transaction: &Transaction) {
        unimplemented!()
    }

    /// Searches the index for the provided key.
    ///
    /// # Parameters
    /// - `key`: The index key.
    /// - `result`: The collection of RIDs that is populated with results of the search.
    /// - `transaction`: The transaction context.
    fn scan_key(&self, key: &Tuple, result: &mut Vec<RID>, transaction: &Transaction) {
        unimplemented!()
    }
}
