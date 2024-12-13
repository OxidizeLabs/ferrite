use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::filter_plan::FilterNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor, TableWithJoins};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

pub struct QueryPlanner {
    // Add fields for catalog/metadata management
}

impl QueryPlanner {
    pub fn new() -> Self {
        Self {}
    }

    pub fn create_plan(&self, sql: &str) -> Result<PlanNode, String> {
        // Parse SQL using sqlparser
        let dialect = GenericDialect {};
        let ast =
            Parser::parse_sql(&dialect, sql).map_err(|e| format!("Failed to parse SQL: {}", e))?;

        if ast.len() != 1 {
            return Err("Expected exactly one statement".to_string());
        }

        match &ast[0] {
            Statement::Query(query) => self.plan_query(query),
            _ => Err("Only SELECT statements are supported".to_string()),
        }
    }

    fn plan_query(&self, query: &Query) -> Result<PlanNode, String> {
        match &*query.body {
            SetExpr::Select(select) => self.plan_select(select),
            _ => Err("Only simple SELECT statements are supported".to_string()),
        }
    }

    fn plan_select(&self, select: &Box<Select>) -> Result<PlanNode, String> {
        if select.from.len() != 1 {
            return Err("Only single table queries are supported".to_string());
        }

        // Plan the base table scan
        let table_scan = self.plan_table_scan(&select.from[0])?;

        // If there's a WHERE clause, add a filter node
        if let Some(where_clause) = &select.selection {
            let filter_expr = self.parse_expression(where_clause)?;
            let filter_schema = table_scan.get_output_schema().clone();

            Ok(PlanNode::Filter(FilterNode::new(
                filter_schema,
                Arc::new(filter_expr),
                table_scan,
            )))
        } else {
            Ok(table_scan)
        }
    }

    fn plan_table_scan(&self, table: &TableWithJoins) -> Result<PlanNode, String> {
        match &table.relation {
            TableFactor::Table { name, .. } => {
                // In a real implementation, you would look up the table in your catalog
                // For now, we'll create a dummy schema
                let schema = Schema::new(vec![
                    Column::new("id", TypeId::Integer),
                    Column::new("name", TypeId::VarChar),
                    Column::new("age", TypeId::Integer),
                ]);

                Ok(PlanNode::SeqScan(SeqScanPlanNode::new(
                    schema,
                    0, // table_oid would come from catalog
                    name.to_string(),
                    None,
                )))
            }
            _ => Err("Only simple table scans are supported".to_string()),
        }
    }

    fn parse_expression(&self, expr: &sqlparser::ast::Expr) -> Result<Expression, String> {
        match expr {
            sqlparser::ast::Expr::BinaryOp { left, op, right } => {
                let left_expr = self.parse_expression(left)?;
                let right_expr = self.parse_expression(right)?;

                let comparison_type = match op {
                    sqlparser::ast::BinaryOperator::Eq => ComparisonType::Equal,
                    sqlparser::ast::BinaryOperator::Gt => ComparisonType::GreaterThan,
                    sqlparser::ast::BinaryOperator::Lt => ComparisonType::LessThan,
                    // Add other operators as needed
                    _ => return Err("Unsupported binary operator".to_string()),
                };

                Ok(Expression::Comparison(ComparisonExpression::new(
                    Arc::new(left_expr),
                    Arc::new(right_expr),
                    comparison_type,
                    vec![],
                )))
            }
            sqlparser::ast::Expr::Value(value) => {
                // Convert SQL value to our Value type
                let val = match value {
                    sqlparser::ast::Value::Number(n, _) => {
                        Value::from(n.parse::<i32>().map_err(|e| e.to_string())?)
                    }
                    sqlparser::ast::Value::SingleQuotedString(s) => Value::from(s.as_str()),
                    // Add other value types as needed
                    _ => return Err("Unsupported value type".to_string()),
                };

                Ok(Expression::Constant(ConstantExpression::new(
                    val,
                    Column::new("const", TypeId::Integer), // Type should match the value
                    vec![],
                )))
            }
            // Add support for other expression types
            _ => Err("Unsupported expression type".to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_select() {
        let planner = QueryPlanner::new();
        let sql = "SELECT * FROM users";
        let plan = planner.create_plan(sql).unwrap();

        assert!(matches!(plan, PlanNode::SeqScan(_)));
    }

    #[test]
    fn test_select_with_filter() {
        let planner = QueryPlanner::new();
        let sql = "SELECT * FROM users WHERE age > 25";
        let plan = planner.create_plan(sql).unwrap();

        assert!(matches!(plan, PlanNode::Filter(_)));
        if let PlanNode::Filter(filter_node) = plan {
            assert!(matches!(filter_node.get_child_plan(), PlanNode::SeqScan(_)));
        }
    }
}
