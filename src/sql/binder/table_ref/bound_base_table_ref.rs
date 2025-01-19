use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::sql::binder::bound_table_ref::{BoundTableRef, TableReferenceType};
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display};

/// A bound table ref type for single table. e.g., `SELECT x FROM y`, where `y` is `BoundBaseTableRef`.
#[derive(Clone)]
pub struct BoundBaseTableRef {
    /// The name of the table.
    table: String,
    /// The oid of the table.
    oid: TableOidT,
    /// The alias of the table.
    alias: Option<String>,
    /// The schema of the table.
    schema: Schema,
}

impl BoundBaseTableRef {
    /// Creates a new BoundBaseTableRef.
    pub fn new(table: String, oid: TableOidT, alias: Option<String>, schema: Schema) -> Self {
        Self {
            table: table.to_string(),
            oid,
            alias,
            schema,
        }
    }

    /// Gets the bound table name (alias if present, otherwise table name).
    pub fn get_bound_table_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }

    pub fn get_schema(&self) -> &Schema {
        &self.schema
    }
}

impl BoundTableRef for BoundBaseTableRef {
    fn table_reference_type(&self) -> TableReferenceType {
        TableReferenceType::BaseTable
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundTableRef> {
        Box::new(self.clone())
    }
}

impl Display for BoundBaseTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.alias {
            Some(alias) => write!(f, "{} AS {}", self.table, alias),
            None => write!(f, "{}", self.table),
        }
    }
}

impl Debug for BoundBaseTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BoundBaseTableRef")
            .field("table_name", &self.table)
            .field("table_oid", &self.oid)
            .field("alias", &self.alias)
            .field("schema", &self.schema)
            .finish()
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn bound_base_table_ref() {
        let schema = Schema::new(vec![]);
        let table_ref =
            BoundBaseTableRef::new("users".to_string(), 1, Some("u".to_string()), schema);

        assert_eq!(
            table_ref.table_reference_type(),
            TableReferenceType::BaseTable
        );
        assert_eq!(table_ref.get_bound_table_name(), "u");
        assert_eq!(table_ref.to_string(), "users AS u");

        let table_ref_no_alias =
            BoundBaseTableRef::new("products".to_string(), 2, None, Schema::new(vec![]));

        assert_eq!(table_ref_no_alias.get_bound_table_name(), "products");
        assert_eq!(table_ref_no_alias.to_string(), "products");
    }
}
