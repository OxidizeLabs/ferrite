use crate::common::statement_type::StatementType;
use crate::sql::binder::bound_expression::BoundExpression;
use crate::sql::binder::bound_order_by::BoundOrderBy;
use crate::sql::binder::bound_statement::BoundStatement;
use crate::sql::binder::bound_table_ref::BoundTableRef;
use crate::sql::binder::table_ref::bound_subquery_ref::BoundSubqueryRef;
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Display};

/// Represents a bound SELECT statement in SQL.
#[derive(Clone)]
pub struct SelectStatement {
    table: Box<dyn BoundTableRef>,
    select_list: Vec<Box<dyn BoundExpression>>,
    where_clause: Option<Box<dyn BoundExpression>>,
    group_by: Vec<Box<dyn BoundExpression>>,
    having: Option<Box<dyn BoundExpression>>,
    limit_count: Option<Box<dyn BoundExpression>>,
    limit_offset: Option<Box<dyn BoundExpression>>,
    sort: Vec<BoundOrderBy>,
    ctes: Vec<BoundSubqueryRef>,
    distinct: bool,
}

impl SelectStatement {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table: Box<dyn BoundTableRef>,
        select_list: Vec<Box<dyn BoundExpression>>,
        where_clause: Option<Box<dyn BoundExpression>>,
        group_by: Vec<Box<dyn BoundExpression>>,
        having: Option<Box<dyn BoundExpression>>,
        limit_count: Option<Box<dyn BoundExpression>>,
        limit_offset: Option<Box<dyn BoundExpression>>,
        sort: Vec<BoundOrderBy>,
        ctes: Vec<BoundSubqueryRef>,
        distinct: bool,
    ) -> Self {
        Self {
            table,
            select_list,
            where_clause,
            group_by,
            having,
            limit_count,
            limit_offset,
            sort,
            ctes,
            distinct,
        }
    }
}

impl BoundStatement for SelectStatement {
    fn statement_type(&self) -> StatementType {
        StatementType::SelectStatement
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl Display for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SELECT ")?;
        if self.distinct {
            write!(f, "DISTINCT ")?;
        }
        for (i, expr) in self.select_list.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            // Remove quotes from column names
            let expr_str = expr.to_string();
            let cleaned_expr = expr_str.trim_matches(|c| c == '"' || c == '`');
            write!(f, "{}", clean_identifier(cleaned_expr))?;
        }
        write!(f, " FROM {}", self.table)?;
        if let Some(where_clause) = &self.where_clause {
            write!(f, " WHERE {}", where_clause)?;
        }
        if !self.group_by.is_empty() {
            write!(f, " GROUP BY ")?;
            for (i, expr) in self.group_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                // Remove quotes from column names in GROUP BY
                let expr_str = expr.to_string();
                let cleaned_expr = expr_str.trim_matches(|c| c == '"' || c == '`');
                write!(f, "{}", cleaned_expr)?;
            }
        }
        if let Some(having) = &self.having {
            write!(f, " HAVING {}", having)?;
        }
        if !self.sort.is_empty() {
            write!(f, " ORDER BY ")?;
            for (i, order_by) in self.sort.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", order_by)?;
            }
        }
        if let Some(limit) = &self.limit_count {
            write!(f, " LIMIT {}", limit)?;
        }
        if let Some(offset) = &self.limit_offset {
            write!(f, " OFFSET {}", offset)?;
        }
        Ok(())
    }
}

impl Debug for SelectStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SelectStatement")
            .field("table", &self.table)
            .field("select_list", &self.select_list.len())
            .field("where_clause", &self.where_clause.is_some())
            .field("group_by", &self.group_by.len())
            .field("having", &self.having.is_some())
            .field("limit_count", &self.limit_count.is_some())
            .field("limit_offset", &self.limit_offset.is_some())
            .field("sort", &self.sort.len())
            .field("ctes", &self.ctes.len())
            .field("distinct", &self.distinct)
            .finish()
    }
}

fn clean_identifier(s: &str) -> String {
    s.trim_matches(|c| c == '"' || c == '`').to_string()
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::sql::binder::bound_table_ref::TableReferenceType;
    use crate::sql::binder::expressions::bound_constant::BoundConstant;

    struct MockBoundTableRef;
    impl BoundTableRef for MockBoundTableRef {
        fn table_reference_type(&self) -> TableReferenceType {
            todo!()
        }

        fn as_any(&self) -> &dyn Any {
            self
        }

        fn clone_box(&self) -> Box<dyn BoundTableRef> {
            todo!()
        }
    }

    impl Display for MockBoundTableRef {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "mock_table")
        }
    }

    #[test]
    fn select_statement() {
        let select_stmt = SelectStatement::new(
            Box::new(MockBoundTableRef),
            vec![
                Box::new(BoundConstant::new("column1")),
                Box::new(BoundConstant::new("column2")),
            ],
            Some(Box::new(BoundConstant::new(true))),
            vec![],
            None,
            Some(Box::new(BoundConstant::new(10))),
            None,
            vec![],
            vec![],
            false,
        );

        assert_eq!(select_stmt.statement_type(), StatementType::SelectStatement);
        assert_eq!(
            select_stmt.to_string(),
            "SELECT column1, column2 FROM mock_table WHERE true LIMIT 10"
        );
    }
}
