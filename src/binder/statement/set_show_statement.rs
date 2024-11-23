use crate::binder::bound_statement::BoundStatement;
use crate::common::statement_type::StatementType;
use std::any::Any;
use std::fmt;
use std::fmt::{Display, Formatter};

/// Represents a bound VARIABLE SET statement.
#[derive(Debug, Clone)]
pub struct VariableSetStatement {
    pub variable: String,
    pub value: String,
}

impl VariableSetStatement {
    /// Creates a new VariableSetStatement.
    ///
    /// # Arguments
    ///
    /// * `variable` - The name of the variable to set.
    /// * `value` - The value to set the variable to.
    pub fn new(variable: String, value: String) -> Self {
        Self { variable, value }
    }
}

impl BoundStatement for VariableSetStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::VariableSetStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for VariableSetStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "BoundVariableSet {{ variable={}, value={} }}",
            self.variable, self.value
        )
    }
}

/// Represents a bound VARIABLE SHOW statement.
#[derive(Debug, Clone)]
pub struct VariableShowStatement {
    pub variable: String,
}

impl VariableShowStatement {
    /// Creates a new VariableShowStatement.
    ///
    /// # Arguments
    ///
    /// * `variable` - The name of the variable to show.
    pub fn new(variable: String) -> Self {
        Self { variable }
    }
}

impl BoundStatement for VariableShowStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::VariableShowStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for VariableShowStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BoundVariableShow {{ variable={} }}", self.variable)
    }
}

/// Represents a bound TRANSACTION statement.
#[derive(Debug, Clone)]
pub struct TransactionStatement {
    pub type_: String,
}

impl TransactionStatement {
    /// Creates a new TransactionStatement.
    ///
    /// # Arguments
    ///
    /// * `type_` - The type of transaction statement.
    pub fn new(type_: String) -> Self {
        Self { type_ }
    }
}

impl BoundStatement for TransactionStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::TransactionStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for TransactionStatement {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "BoundTransaction {{ type={} }}", self.type_)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn variable_set_statement() {
        let stmt = VariableSetStatement::new("my_var".to_string(), "42".to_string());
        assert_eq!(stmt.statement_type(), StatementType::VariableSetStatement);
        assert_eq!(
            stmt.to_string(),
            "BoundVariableSet { variable=my_var, value=42 }"
        );
    }

    #[test]
    fn variable_show_statement() {
        let stmt = VariableShowStatement::new("my_var".to_string());
        assert_eq!(stmt.statement_type(), StatementType::VariableShowStatement);
        assert_eq!(stmt.to_string(), "BoundVariableShow { variable=my_var }");
    }

    #[test]
    fn transaction_statement() {
        let stmt = TransactionStatement::new("BEGIN".to_string());
        assert_eq!(stmt.statement_type(), StatementType::TransactionStatement);
        assert_eq!(stmt.to_string(), "BoundTransaction { type=BEGIN }");
    }
}
