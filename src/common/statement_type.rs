use std::fmt;

/// Represents different types of SQL statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementType {
    InvalidStatement,
    SelectStatement,
    InsertStatement,
    UpdateStatement,
    CreateStatement,
    DeleteStatement,
    ExplainStatement,
    DropStatement,
    IndexStatement,
    VariableSetStatement,
    VariableShowStatement,
    TransactionStatement,
}

impl StatementType {
    /// Returns a string representation of the StatementType.
    pub fn as_str(&self) -> &'static str {
        match self {
            StatementType::InvalidStatement => "Invalid",
            StatementType::SelectStatement => "Select",
            StatementType::InsertStatement => "Insert",
            StatementType::UpdateStatement => "Update",
            StatementType::CreateStatement => "Create",
            StatementType::DeleteStatement => "Delete",
            StatementType::ExplainStatement => "Explain",
            StatementType::DropStatement => "Drop",
            StatementType::IndexStatement => "Index",
            StatementType::VariableShowStatement => "VariableShow",
            StatementType::VariableSetStatement => "VariableSet",
            StatementType::TransactionStatement => "Transaction",
        }
    }
}

impl fmt::Display for StatementType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_type_display() {
        assert_eq!(format!("{}", StatementType::SelectStatement), "Select");
        assert_eq!(format!("{}", StatementType::CreateStatement), "Create");
        assert_eq!(format!("{}", StatementType::InvalidStatement), "Invalid");
    }
}