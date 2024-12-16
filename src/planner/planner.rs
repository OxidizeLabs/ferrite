use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::catalogue::catalogue::Catalog;
use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::concurrency::transaction::{IsolationLevel, Transaction};
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::create_plan::CreateTablePlanNode;
use crate::execution::plans::filter_plan::FilterNode;
use crate::execution::plans::insert_plan::InsertNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::execution::plans::values_plan::ValuesNode;
use crate::recovery::log_manager::LogManager;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use log::{debug, info};
use parking_lot::{Mutex, RwLock};
use sqlparser::ast::{Expr, Value as SqlValue};
use sqlparser::ast::Query as SqlQuery;
use sqlparser::ast::{ColumnDef, CreateTable, DataType, Insert, ObjectName, Query, Select, SetExpr, Statement, TableFactor, TableWithJoins, Values};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::env;
use std::sync::Arc;


pub struct QueryPlanner {
    catalog: Arc<RwLock<Catalog>>,
    log_detailed: bool,
}

impl QueryPlanner {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            catalog,
            log_detailed: env::var("RUST_TEST").is_ok(),
        }
    }

    pub fn with_detailed_logging(catalog: Arc<RwLock<Catalog>>, detailed: bool) -> Self {
        Self {
            catalog,
            log_detailed: detailed || env::var("RUST_TEST").is_ok(),
        }
    }

    pub fn set_detailed_logging(&mut self, detailed: bool) {
        self.log_detailed = detailed || env::var("RUST_TEST").is_ok();
    }

    pub fn create_plan(&mut self, sql: &str) -> Result<PlanNode, String> {
        info!("Planning query: {}", sql);

        let dialect = GenericDialect {};
        let ast = match Parser::parse_sql(&dialect, sql) {
            Ok(ast) => ast,
            Err(e) => {
                info!("Failed to parse SQL: {}", e);
                return Err(format!("Failed to parse SQL: {}", e));
            }
        };

        if ast.len() != 1 {
            info!("Error: Expected exactly one statement");
            return Err("Expected exactly one statement".to_string());
        }

        if self.log_detailed {
            debug!("Parsed AST: {:?}", ast[0]);
        }

        match &ast[0] {
            Statement::CreateTable(create_table) => {
                debug!("Planning CREATE TABLE statement");
                let txn = Arc::new(Mutex::new(Transaction::new(0, IsolationLevel::ReadUncommitted)));
                self.plan_create_table(create_table, &txn)
            }
            Statement::Query(query) => {
                debug!("Planning SELECT query");
                self.plan_query(query)
            }
            Statement::Insert(insert) => {
                debug!("Planning INSERT statement");
                let txn = Arc::new(Mutex::new(Transaction::new(0, IsolationLevel::ReadUncommitted)));
                self.plan_insert(insert)
            }
            _ => {
                info!("Error: Unsupported statement type");
                Err("Only SELECT statements are supported".to_string())
            }
        }
    }

    fn plan_create_table(
        &mut self,
        create_table: &CreateTable,
        txn: &Arc<Mutex<Transaction>>,
    ) -> Result<PlanNode, String> {
        let columns = self.convert_column_defs(&create_table.columns)?;
        let schema = Schema::new(columns);
        let table_name = create_table.name.to_string();

        // Add the new table metadata to the catalog
        let mut catalog_guard = self.catalog.write();
        if let Some(_) = catalog_guard.create_table(txn, &table_name, schema.clone(), true) {
            Ok(PlanNode::CreateTable(CreateTablePlanNode::new(
                schema,
                table_name,
                create_table.if_not_exists,
            )))
        } else {
            Err(format!("Table '{}' already exists", table_name))
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

    fn plan_insert(&self, insert: &Insert) -> Result<PlanNode, String> {
        // Extract table name
        let table_name = self.extract_table_name(&insert.table_name)?;

        // Check table existence in the catalog
        let catalog_guard = self.catalog.read();
        let table_info = catalog_guard
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' does not exist in the catalog", table_name))?;

        // Fetch schema and OID
        let table_schema = table_info.get_table_schema();
        let table_oid = table_info.get_table_oidt();

        // Plan values or select source
        let child_plan = match &insert.source {
            Some(query) => match &*query.body { // Dereference the Box here
                SetExpr::Values(values) => {
                    self.plan_values_node(&values.rows, &table_schema)?
                }
                SetExpr::Select(select) => {
                    let select_plan = self.plan_select(select)?;
                    if !self.schemas_compatible(select_plan.get_output_schema(), &table_schema) {
                        return Err("SELECT schema does not match INSERT target schema".to_string());
                    }
                    select_plan
                }
                _ => return Err("Only VALUES and SELECT are supported in INSERT statements".to_string()),
            },
            None => return Err("INSERT statement must have a source (VALUES or SELECT)".to_string()),
        };

        // Return constructed insert plan node
        Ok(PlanNode::Insert(InsertNode::new(
            table_schema,
            table_oid,
            table_name,
            vec![],
            child_plan,
        )))
    }

    fn extract_table_name(&self, table_name: &ObjectName) -> Result<String, String> {
        match table_name {
            ObjectName(parts) if parts.len() == 1 => Ok(parts[0].value.clone()),
            _ => Err("Only single table INSERT statements are supported".to_string()),
        }
    }

    fn plan_values_node(&self, rows: &[Vec<Expr>], schema: &Schema) -> Result<PlanNode, String> {
        let column_count = schema.get_column_count() as usize;
        let mut all_rows = Vec::new();

        for row in rows {
            if row.len() != column_count {
                return Err(format!(
                    "INSERT has {} values but {} columns",
                    row.len(),
                    column_count
                ));
            }

            let row_expressions: Result<Vec<Arc<Expression>>, String> = row
                .iter()
                .enumerate()
                .map(|(column_index, expr)| {
                    let column = schema
                        .get_column(column_index)
                        .ok_or_else(|| format!("Failed to get column at index {} from schema", column_index))?;
                    self.parse_value_expression(expr, column)
                })
                .collect();

            all_rows.push(row_expressions?);
        }

        Ok(PlanNode::Values(ValuesNode::new(
            schema.clone(),
            all_rows,
            PlanNode::Empty,
        ).unwrap()))
    }

    fn parse_value_expression(&self, expr: &Expr, column: &Column) -> Result<Arc<Expression>, String> {
        let expression = match expr {
            Expr::Value(value) => {
                Expression::Constant(self.create_constant_expression(value, column)?)
            }
            Expr::Identifier(ident) => {
                Expression::Constant(self.create_string_expression(&ident.value, column)?)
            }
            _ => self.parse_expression(expr)?,
        };
        Ok(Arc::new(expression))
    }

    fn plan_table_scan(&self, table: &TableWithJoins) -> Result<PlanNode, String> {
        match &table.relation {
            TableFactor::Table { name, .. } => {
                // Table name as a string
                let table_name = name.to_string();

                // Fetch the schema from the catalog
                let schema = self
                    .catalog.read()
                    .get_table_schema(&table_name)
                    .ok_or_else(|| format!("Table '{}' not found in the catalog", table_name))?;

                // Use the real schema and include table_oid from the catalog
                let table_oid = self
                    .catalog.read()
                    .get_table(&table_name)
                    .ok_or_else(|| format!("Table OID for '{}' not found in the catalog", table_name))?
                    .get_table_oidt();

                Ok(PlanNode::SeqScan(SeqScanPlanNode::new(
                    schema,
                    table_oid,
                    table_name,
                    None,
                )))
            }
            _ => Err("Only simple table scans are supported".to_string()),
        }
    }

    fn parse_expression(&self, expr: &Expr) -> Result<Expression, String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
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
            Expr::Value(value) => {
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

    fn schemas_compatible(&self, source: &Schema, target: &Schema) -> bool {
        if source.get_column_count() != target.get_column_count() {
            return false;
        }

        for i in 0..source.get_column_count() {
            let source_col = source.get_column(i as usize).unwrap();
            let target_col = target.get_column(i as usize).unwrap();

            if !self.types_compatible(source_col.get_type(), target_col.get_type()) {
                return false;
            }
        }

        true
    }

    fn types_compatible(&self, source_type: TypeId, target_type: TypeId) -> bool {
        // Add your type compatibility rules here
        // For example:
        match (source_type, target_type) {
            // Same types are always compatible
            (a, b) if a == b => true,
            _ => false,
        }
    }

    fn create_numeric_expression(&self, value: &str, column: &Column) -> Result<ConstantExpression, String> {
        match column.get_type() {
            TypeId::Integer => {
                let num = value.parse::<i32>()
                    .map_err(|_| format!("Failed to parse {} as integer", value))?;
                Ok(ConstantExpression::new(
                    Value::new(num),
                    column.clone(),
                    Vec::new(),
                ))
            }
            _ => Err(format!("Cannot convert number to {:?}", column.get_type()))
        }
    }

    fn create_string_expression(&self, value: &str, column: &Column) -> Result<ConstantExpression, String> {
        if column.get_type() != TypeId::VarChar {
            return Err(format!("Cannot insert string into column of type {:?}", column.get_type()));
        }
        Ok(ConstantExpression::new(
            Value::new(value.to_string()),
            column.clone(),
            Vec::new(),
        ))
    }

    fn create_boolean_expression(&self, value: bool, column: &Column) -> Result<ConstantExpression, String> {
        if column.get_type() != TypeId::Boolean {
            return Err(format!("Cannot insert boolean into column of type {:?}", column.get_type()));
        }
        Ok(ConstantExpression::new(
            Value::new(value),
            column.clone(),
            Vec::new(),
        ))
    }

    fn create_null_expression(&self, column: &Column) -> Result<ConstantExpression, String> {
        Ok(ConstantExpression::new(
            Value::new(Val::Null),
            column.clone(),
            Vec::new(),
        ))
    }

    fn create_constant_expression(
        &self,
        value: &sqlparser::ast::Value,
        column: &Column,
    ) -> Result<ConstantExpression, String> {
        match value {
            sqlparser::ast::Value::Number(number, _) => self.create_numeric_expression(number, column),
            sqlparser::ast::Value::SingleQuotedString(string)
            | sqlparser::ast::Value::DoubleQuotedString(string) => {
                self.create_string_expression(string, column)
            },
            sqlparser::ast::Value::Boolean(boolean) => self.create_boolean_expression(*boolean, column),
            sqlparser::ast::Value::Null => self.create_null_expression(column),
            _ => Err(format!("Unsupported SQL value type: {:?}", value)),
        }
    }

}

// pub struct TestContext {
//     pub catalog: Arc<RwLock<Catalog>>,
//     pub planner: QueryPlanner,
// }
//
// impl TestContext {
//     pub fn new(test_name: &str) -> Self {
//         // Initialize mock BufferPoolManager, LogManager, Catalog
//         let bpm = Arc::new(BufferPoolManager::new(/* params */));
//         let log_manager = Arc::new(LogManager::new(/* params */));
//
//         let catalog = Arc::new(RwLock::new(Catalog::new(
//             bpm,
//             log_manager,
//             0, // Initial table OID
//             0, // Initial index OID
//             Default::default(),
//             Default::default(),
//             Default::default(),
//             Default::default(),
//         )));
//
//         let planner = QueryPlanner::new(Arc::clone(&catalog));
//
//         Self { catalog, planner }
//     }
// }
//
// #[cfg(test)]
// mod tests {
//     use std::env;
//     use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
//     use crate::planner::planner::QueryPlanner;
//     use crate::types_db::type_id::TypeId;
//
//     #[test]
//     fn test_query_planner_logging() {
//             env::set_var("RUST_TEST", "1");
//
//             let mut planner = QueryPlanner::with_detailed_logging(true);
//
//             // Test CREATE TABLE logging
//             let create_sql = "CREATE TABLE users (id INT, name VARCHAR(255))";
//             let result = planner.create_plan(create_sql);
//             assert!(result.is_ok());
//
//             // Test SELECT logging
//             let select_sql = "SELECT * FROM users WHERE id > 10";
//             let result = planner.create_plan(select_sql);
//             assert!(result.is_ok());
//
//             // Test error logging
//             let invalid_sql = "INVALID SQL";
//             let result = planner.create_plan(invalid_sql);
//             assert!(result.is_err());
//         }
//
//     #[test]
//     fn test_log_level_changes() {
//             env::set_var("RUST_TEST", "1");
//
//             let mut planner = QueryPlanner::new();
//
//             // Test with basic logging
//             let sql = "SELECT * FROM users";
//             let _ = planner.create_plan(sql);
//
//             // Change to detailed logging
//             planner.set_detailed_logging(true);
//             let _ = planner.create_plan(sql);
//         }
//
//
//     #[test]
//     fn test_create_table_plan() {
//         let planner = QueryPlanner::new();
//         let sql = "CREATE TABLE users (id INT, name VARCHAR(255), age INT)";
//         let plan = planner.create_plan(sql).unwrap();
//
//         match plan {
//             PlanNode::CreateTable(create_table) => {
//                 assert_eq!(create_table.get_table_name(), "users");
//                 assert_eq!(create_table.if_not_exists(), false);
//
//                 let schema = create_table.get_output_schema();
//                 assert_eq!(schema.get_column_count(), 3);
//
//                 let columns = schema.get_columns();
//                 assert_eq!(columns[0].get_name(), "id");
//                 assert_eq!(columns[0].get_type(), TypeId::Integer);
//
//                 assert_eq!(columns[1].get_name(), "name");
//                 assert_eq!(columns[1].get_type(), TypeId::VarChar);
//
//                 assert_eq!(columns[2].get_name(), "age");
//                 assert_eq!(columns[2].get_type(), TypeId::Integer);
//             }
//             _ => panic!("Expected CreateTable plan node"),
//         }
//     }
//
//     #[test]
//     fn test_create_table_if_not_exists() {
//         let planner = QueryPlanner::new();
//         let sql = "CREATE TABLE IF NOT EXISTS users (id INT)";
//         let plan = planner.create_plan(sql).unwrap();
//
//         match plan {
//             PlanNode::CreateTable(create_table) => {
//                 assert_eq!(create_table.get_table_name(), "users");
//                 assert_eq!(create_table.if_not_exists(), true);
//             }
//             _ => panic!("Expected CreateTable plan node"),
//         }
//     }
//
//     #[test]
//     fn test_simple_select() {
//         let planner = QueryPlanner::new();
//         let sql = "SELECT * FROM users";
//         let plan = planner.create_plan(sql).unwrap();
//
//         assert!(matches!(plan, PlanNode::SeqScan(_)));
//     }
//
//     #[test]
//     fn test_select_with_filter() {
//         let planner = QueryPlanner::new();
//         let sql = "SELECT * FROM users WHERE age > 25";
//         let plan = planner.create_plan(sql).unwrap();
//
//         assert!(matches!(plan, PlanNode::Filter(_)));
//         if let PlanNode::Filter(filter_node) = plan {
//             assert!(matches!(filter_node.get_child_plan(), PlanNode::SeqScan(_)));
//         }
//     }
// }
