use crate::binder::bound_statement::BoundStatement;
use crate::catalogue::column::Column;
use crate::common::statement_type::StatementType;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display, Formatter};

/// Represents a CREATE TABLE statement in SQL.
#[derive(Debug)]
pub struct CreateStatement {
    /// The name of the table to be created.
    pub table: String,
    /// The columns of the table.
    pub columns: Vec<Column>,
    /// The primary key columns.
    pub primary_key: Vec<String>,
}

impl CreateStatement {
    /// Creates a new CreateStatement.
    pub fn new(table: String, columns: Vec<Column>, primary_key: Vec<String>) -> Self {
        Self {
            table,
            columns,
            primary_key,
        }
    }
}

impl BoundStatement for CreateStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::CreateStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for CreateStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        writeln!(f, "CREATE TABLE {} (", self.table)?;
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", column)?;
        }
        if !self.primary_key.is_empty() {
            write!(f, ", PRIMARY KEY ({}))", self.primary_key.join(", "))?;
        } else {
            write!(f, ")")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn create_statement() {
        let create_stmt = CreateStatement::new(
            "users".to_string(),
            vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ],
            vec!["id".to_string()],
        );

        assert_eq!(create_stmt.statement_type(), StatementType::CreateStatement);
        assert_eq!(
            create_stmt.to_string(),
            "CREATE TABLE users (\nid, name, PRIMARY KEY (id))"
        );

        let create_stmt_no_pk = CreateStatement::new(
            "products".to_string(),
            vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ],
            vec![],
        );

        assert_eq!(
            create_stmt_no_pk.to_string(),
            "CREATE TABLE products (\nid, name)"
        );
    }
}
