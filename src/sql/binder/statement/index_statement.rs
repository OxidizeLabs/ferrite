use crate::sql::binder::bound_statement::BoundStatement;
use crate::sql::binder::expressions::bound_column_ref::BoundColumnRef;
use crate::sql::binder::table_ref::bound_base_table_ref::BoundBaseTableRef;
use crate::common::statement_type::StatementType;
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents a bound CREATE INDEX statement.
#[derive(Debug)]
pub struct IndexStatement {
    /// Name of the index
    pub index_name: String,
    /// Table on which the index is created
    pub table: Box<BoundBaseTableRef>,
    /// Columns included in the index
    pub cols: Vec<BoundColumnRef>,
    /// Type of the index (e.g., "btree", "hash")
    pub index_type: String,
    /// Column-specific options
    pub col_options: Vec<String>,
    /// General index options
    pub options: Vec<(String, i32)>,
}

impl IndexStatement {
    /// Creates a new IndexStatement.
    ///
    /// # Arguments
    ///
    /// * `index_name` - Name of the index
    /// * `table` - Table on which the index is created
    /// * `cols` - Columns included in the index
    /// * `index_type` - Type of the index
    /// * `col_options` - Column-specific options
    /// * `options` - General index options
    ///
    /// # Returns
    ///
    /// A new IndexStatement
    pub fn new(
        index_name: String,
        table: Box<BoundBaseTableRef>,
        cols: Vec<BoundColumnRef>,
        index_type: String,
        col_options: Vec<String>,
        options: Vec<(String, i32)>,
    ) -> Self {
        Self {
            index_name,
            table,
            cols,
            index_type,
            col_options,
            options,
        }
    }
}

impl BoundStatement for IndexStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::IndexStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for IndexStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE INDEX {} ON {} (", self.index_name, self.table)?;
        for (i, col) in self.cols.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", col)?;
        }
        write!(f, ") USING {}", self.index_type)?;
        if !self.col_options.is_empty() {
            write!(f, " ({})", self.col_options.join(", "))?;
        }
        if !self.options.is_empty() {
            write!(f, " WITH (")?;
            for (i, (key, value)) in self.options.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{} = {}", key, value)?;
            }
            write!(f, ")")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::sql::binder::bound_statement::AnyBoundStatement;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn index_statement_creation() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let stmt = IndexStatement::new(
            "idx_test".to_string(),
            Box::new(BoundBaseTableRef::new("users".to_string(), 0, None, schema)),
            vec![BoundColumnRef::new(vec!["id".to_string()])],
            "btree".to_string(),
            vec![],
            vec![("fillfactor".to_string(), 90)],
        );

        assert_eq!(stmt.index_name, "idx_test");
        assert_eq!(stmt.index_type, "btree");
        assert_eq!(stmt.cols.len(), 1);
        assert_eq!(stmt.options.len(), 1);
    }

    #[test]
    fn index_statement_display() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let stmt = IndexStatement::new(
            "idx_test".to_string(),
            Box::new(BoundBaseTableRef::new("users".to_string(), 0, None, schema)),
            vec![
                BoundColumnRef::new(vec!["id".to_string()]),
                BoundColumnRef::new(vec!["name".to_string()]),
            ],
            "btree".to_string(),
            vec!["DESC".to_string()],
            vec![("fillfactor".to_string(), 90)],
        );

        let expected =
            "CREATE INDEX idx_test ON users (id, name) USING btree (DESC) WITH (fillfactor = 90)";
        assert_eq!(stmt.to_string(), expected);
    }

    #[test]
    fn index_statement_bound_statement() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let stmt = IndexStatement::new(
            "idx_test".to_string(),
            Box::new(BoundBaseTableRef::new("users".to_string(), 0, None, schema)),
            vec![BoundColumnRef::new(vec!["id".to_string()])],
            "btree".to_string(),
            vec![],
            vec![],
        );

        assert_eq!(stmt.statement_type(), StatementType::IndexStatement);

        // Test that we can use it as AnyBoundStatement
        let any_stmt: &dyn AnyBoundStatement = &stmt;
        assert_eq!(
            any_stmt.as_bound_statement().statement_type(),
            StatementType::IndexStatement
        );
    }
}
