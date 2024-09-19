use crate::binder::bound_table_ref::{BoundTableRef, TableReferenceType};
use crate::binder::statement::select_statement::SelectStatement;
use std::any::Any;
use std::fmt;
use std::fmt::Display;

/// Represents a subquery. e.g., `SELECT * FROM (SELECT * FROM t1)`,
/// where `(SELECT * FROM t1)` is `BoundSubqueryRef`.
#[derive(Clone)]
pub struct BoundSubqueryRef {
    /// Subquery.
    subquery: Box<SelectStatement>,
    /// Name of each item in the select list.
    select_list_name: Vec<Vec<String>>,
    /// Alias.
    alias: String,
}

impl BoundSubqueryRef {
    /// Creates a new BoundSubqueryRef.
    pub fn new(subquery: Box<SelectStatement>, select_list_name: Vec<Vec<String>>, alias: String) -> Self {
        Self {
            subquery,
            select_list_name,
            alias,
        }
    }

    pub fn get_select_list_name(&self) -> &Vec<Vec<String>> {
        &self.select_list_name
    }
}

impl BoundTableRef for BoundSubqueryRef {
    fn table_reference_type(&self) -> TableReferenceType {
        TableReferenceType::Subquery
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BoundTableRef> {
        Box::new(self.clone())
    }
}

impl Display for BoundSubqueryRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}) AS {}", self.subquery, self.alias)
    }
}

/// CTEList is a vector of BoundSubqueryRef.
pub type CTEList = Vec<Box<BoundSubqueryRef>>;

#[cfg(test)]
mod unit_tests {
    use crate::binder::expressions::bound_constant::BoundConstant;
    use super::*;

    #[test]
    fn bound_subquery_ref() {
        let subquery = Box::new(SelectStatement::new(
            Box::new(MockBoundTableRef),
            vec![Box::new(BoundConstant::new("column1"))],
            None,
            vec![],
            None,
            None,
            None,
            vec![],
            vec![],
            false,
        ));

        let subquery_ref = BoundSubqueryRef::new(
            subquery,
            vec![vec!["column1".to_string()]],
            "subq".to_string(),
        );

        assert_eq!(subquery_ref.table_reference_type(), TableReferenceType::Subquery);
        assert_eq!(
            subquery_ref.to_string(),
            "(SELECT column1 FROM mock_table) AS subq"
        );
    }

    #[derive(Clone)]
    struct MockBoundTableRef;

    impl BoundTableRef for MockBoundTableRef {
        fn table_reference_type(&self) -> TableReferenceType {
            todo!()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_box(&self) -> Box<dyn BoundTableRef> {
            Box::new(self.clone())
        }
    }
    impl Display for MockBoundTableRef {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "mock_table")
        }
    }
}