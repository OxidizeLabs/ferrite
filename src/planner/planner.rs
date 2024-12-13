use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::filter_plan::FilterNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;

use crate::execution::plans::create_plan::CreateTablePlanNode;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use sqlparser::ast::{
    ColumnDef, CreateTable, DataType, Query, Select, SetExpr, Statement, TableFactor,
    TableWithJoins,
};
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
            Statement::CreateTable(create_table) => self.plan_create_table(create_table),
            Statement::Query(query) => self.plan_query(query),
            _ => Err("Only SELECT statements are supported".to_string()),
        }
    }

    fn plan_create_table(&self, create_table: &CreateTable) -> Result<PlanNode, String> {
        let table_name = create_table.name.to_string();
        let columns = self.convert_column_defs(&create_table.columns)?;
        let schema = Schema::new(columns);

        Ok(PlanNode::CreateTable(CreateTablePlanNode::new(
            schema,
            table_name,
            create_table.if_not_exists,
        )))
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

    fn convert_column_defs(&self, column_defs: &[ColumnDef]) -> Result<Vec<Column>, String> {
        let mut columns = Vec::new();

        for col_def in column_defs {
            let column_name = col_def.name.to_string();
            let type_id = self.convert_sql_type(&col_def.data_type)?;

            // Handle VARCHAR/STRING types specifically with length
            let column = match &col_def.data_type {
                DataType::Varchar(_) | DataType::String(_) => {
                    // Default length for variable length types
                    Column::new_varlen(&column_name, type_id, 255)
                }
                _ => Column::new(&column_name, type_id),
            };

            columns.push(column);
        }

        Ok(columns)
    }

    fn convert_sql_type(&self, sql_type: &DataType) -> Result<TypeId, String> {
        match sql_type {
            DataType::Boolean => Ok(TypeId::Boolean),
            DataType::TinyInt(_) => Ok(TypeId::TinyInt),
            DataType::SmallInt(_) => Ok(TypeId::SmallInt),
            DataType::Int(_) | DataType::Integer(_) => Ok(TypeId::Integer),
            DataType::BigInt(_) => Ok(TypeId::BigInt),
            DataType::Decimal(_) => Ok(TypeId::Decimal),
            DataType::Varchar(_) | DataType::String(_) => Ok(TypeId::VarChar),
            _ => Err(format!("Unsupported SQL type: {:?}", sql_type)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_table_plan() {
        let planner = QueryPlanner::new();
        let sql = "CREATE TABLE users (id INT, name VARCHAR(255), age INT)";
        let plan = planner.create_plan(sql).unwrap();

        match plan {
            PlanNode::CreateTable(create_table) => {
                assert_eq!(create_table.get_table_name(), "users");
                assert_eq!(create_table.if_not_exists(), false);

                let schema = create_table.get_output_schema();
                assert_eq!(schema.get_column_count(), 3);

                let columns = schema.get_columns();
                assert_eq!(columns[0].get_name(), "id");
                assert_eq!(columns[0].get_type(), TypeId::Integer);

                assert_eq!(columns[1].get_name(), "name");
                assert_eq!(columns[1].get_type(), TypeId::VarChar);

                assert_eq!(columns[2].get_name(), "age");
                assert_eq!(columns[2].get_type(), TypeId::Integer);
            }
            _ => panic!("Expected CreateTable plan node"),
        }
    }

    #[test]
    fn test_create_table_if_not_exists() {
        let planner = QueryPlanner::new();
        let sql = "CREATE TABLE IF NOT EXISTS users (id INT)";
        let plan = planner.create_plan(sql).unwrap();

        match plan {
            PlanNode::CreateTable(create_table) => {
                assert_eq!(create_table.get_table_name(), "users");
                assert_eq!(create_table.if_not_exists(), true);
            }
            _ => panic!("Expected CreateTable plan node"),
        }
    }

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
