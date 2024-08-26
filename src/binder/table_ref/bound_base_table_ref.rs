use std::fmt;

use crate::binder::bound_table_ref::{BoundTableRef, TableReferenceType};
use crate::catalogue::schema::Schema;
use crate::common::config::TableOidT;

/// A bound table ref type for single table. e.g., `SELECT x FROM y`, where `y` is `BoundBaseTableRef`.
pub struct BoundBaseTableRef {
    /// The name of the table.
    pub table: String,
    /// The oid of the table.
    pub oid: TableOidT,
    /// The alias of the table.
    pub alias: Option<String>,
    /// The schema of the table.
    pub schema: Schema,
}

impl BoundBaseTableRef {
    /// Creates a new BoundBaseTableRef.
    pub fn new(table: String, oid: TableOidT, alias: Option<String>, schema: Schema) -> Self {
        Self {
            table,
            oid,
            alias,
            schema,
        }
    }

    /// Gets the bound table name (alias if present, otherwise table name).
    pub fn get_bound_table_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}

impl BoundTableRef for BoundBaseTableRef {
    fn table_reference_type(&self) -> TableReferenceType {
        TableReferenceType::BaseTable
    }
}

impl fmt::Display for BoundBaseTableRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.alias {
            Some(alias) => write!(
                f,
                "BoundBaseTableRef {{ table={}, oid={}, alias={} }}",
                self.table, self.oid, alias
            ),
            None => write!(
                f,
                "BoundBaseTableRef {{ table={}, oid={} }}",
                self.table, self.oid
            ),
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn bound_base_table_ref() {
        let schema = Schema::new(vec![]); // Assuming Schema can be created with an empty vector
        let table_ref = BoundBaseTableRef::new(
            "users".to_string(),
            1,
            Some("u".to_string()),
            schema,
        );

        assert_eq!(table_ref.table_reference_type(), TableReferenceType::BaseTable);
        assert_eq!(table_ref.get_bound_table_name(), "u");
        assert_eq!(
            table_ref.to_string(),
            "BoundBaseTableRef { table=users, oid=1, alias=u }"
        );

        let table_ref_no_alias = BoundBaseTableRef::new(
            "products".to_string(),
            2,
            None,
            Schema::new(vec![]),
        );

        assert_eq!(table_ref_no_alias.get_bound_table_name(), "products");
        assert_eq!(
            table_ref_no_alias.to_string(),
            "BoundBaseTableRef { table=products, oid=2 }"
        );
    }
}