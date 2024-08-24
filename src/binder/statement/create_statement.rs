use std::fmt;

use crate::binder::bound_statement::{BoundStatement, DefaultBoundStatement};
use crate::catalogue::column::Column;
use crate::common::statement_type::StatementType;
use crate::types_db::type_id::TypeId;

/// Represents a CREATE TABLE statement in SQL.
pub struct CreateStatement {
    base: DefaultBoundStatement,
    table: String,
    columns: Vec<Column>,
    primary_key: Vec<String>,
}

impl CreateStatement {
    /// Creates a new CreateStatement.
    pub fn new(table: String, columns: Vec<Column>, primary_key: Vec<String>) -> Self {
        Self {
            base: DefaultBoundStatement::new(StatementType::CreateStatement),
            table,
            columns,
            primary_key,
        }
    }
}

impl BoundStatement for CreateStatement {
    fn statement_type(&self) -> StatementType {
        self.base.statement_type()
    }
}

impl fmt::Display for CreateStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE TABLE {} (", self.table)?;

        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", column.get_name())?;
        }

        if !self.primary_key.is_empty() {
            write!(f, ", PRIMARY KEY (")?;
            for (i, key) in self.primary_key.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", key)?;
            }
            write!(f, ")")?;
        }

        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_statement_to_string() {
        let stmt = CreateStatement::new(
            "users".to_string(),
            vec![
                Column::new("id".to_string(), TypeId::Integer),
                Column::new("name".to_string(), TypeId::Boolean),
            ],
            vec!["id".to_string()],
        );

        assert_eq!(
            stmt.to_string(),
            "CREATE TABLE users (id, name, PRIMARY KEY (id))"
        );
    }
}