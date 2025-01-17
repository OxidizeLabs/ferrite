use std::fmt;

/// Join types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// Invalid join type.
    Invalid,
    /// Left join.
    Left,
    /// Right join.
    Right,
    /// Inner join.
    Inner,
    /// Outer join.
    Outer,
}

impl fmt::Display for JoinType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JoinType::Invalid => write!(f, "Invalid"),
            JoinType::Left => write!(f, "Left"),
            JoinType::Right => write!(f, "Right"),
            JoinType::Inner => write!(f, "Inner"),
            JoinType::Outer => write!(f, "Outer"),
        }
    }
}

/// Table reference types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TableReferenceType {
    /// Join reference type.
    Join,
    // Add other reference types as needed
}

/// Trait for bound table references.
pub trait BoundTableRef: fmt::Debug {
    /// Returns a string representation of the bound table reference.
    fn to_string(&self) -> String;
}

/// Trait for bound expressions.
pub trait BoundExpression: fmt::Debug {
    /// Returns a string representation of the bound expression.
    fn to_string(&self) -> String;
}

/// A join. e.g., `SELECT * FROM x INNER JOIN y ON ...`, where `x INNER JOIN y ON ...` is `BoundJoinRef`.
#[derive(Debug)]
pub struct BoundJoinRef {
    /// Type of join.
    pub join_type: JoinType,
    /// The left side of the join.
    pub left: Box<dyn BoundTableRef>,
    /// The right side of the join.
    pub right: Box<dyn BoundTableRef>,
    /// Join condition.
    pub condition: Box<dyn BoundExpression>,
}

impl BoundJoinRef {
    /// Creates a new `BoundJoinRef`.
    pub fn new(
        join_type: JoinType,
        left: Box<dyn BoundTableRef>,
        right: Box<dyn BoundTableRef>,
        condition: Box<dyn BoundExpression>,
    ) -> Self {
        Self {
            join_type,
            left,
            right,
            condition,
        }
    }
}

impl BoundTableRef for BoundJoinRef {
    fn to_string(&self) -> String {
        format!(
            "BoundJoin {{ type={}, left={}, right={}, condition={} }}",
            self.join_type,
            self.left.to_string(),
            self.right.to_string(),
            self.condition.to_string()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Mock implementations for testing
    #[derive(Debug)]
    struct MockBoundTableRef;
    impl BoundTableRef for MockBoundTableRef {
        fn to_string(&self) -> String {
            "MockTable".to_string()
        }
    }

    #[derive(Debug)]
    struct MockBoundExpression;
    impl BoundExpression for MockBoundExpression {
        fn to_string(&self) -> String {
            "MockCondition".to_string()
        }
    }

    #[test]
    fn test_join_type_display() {
        assert_eq!(JoinType::Inner.to_string(), "Inner");
        assert_eq!(JoinType::Left.to_string(), "Left");
        assert_eq!(JoinType::Right.to_string(), "Right");
        assert_eq!(JoinType::Outer.to_string(), "Outer");
        assert_eq!(JoinType::Invalid.to_string(), "Invalid");
    }

    #[test]
    fn test_bound_join_ref() {
        let join = BoundJoinRef::new(
            JoinType::Inner,
            Box::new(MockBoundTableRef),
            Box::new(MockBoundTableRef),
            Box::new(MockBoundExpression),
        );

        assert_eq!(join.join_type, JoinType::Inner);
        assert_eq!(
            join.to_string(),
            "BoundJoin { type=Inner, left=MockTable, right=MockTable, condition=MockCondition }"
        );
    }
}
