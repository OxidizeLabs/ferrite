use crate::catalogue::catalogue::Catalog;
use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
use crate::execution::expressions::constant_value_expression::ConstantExpression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode};
use crate::execution::plans::create_plan::CreateTablePlanNode;
use crate::execution::plans::filter_plan::FilterNode;
use crate::execution::plans::insert_plan::InsertNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::execution::plans::values_plan::ValuesNode;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use log::debug;
use parking_lot::RwLock;
use sqlparser::ast::Expr;
use sqlparser::ast::{
    ColumnDef, CreateTable, DataType, Insert, ObjectName, Query, Select, SetExpr, Statement,
    TableFactor, TableWithJoins,
};
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
        debug!("Planning query: {}", sql);

        let dialect = GenericDialect {};
        let ast = match Parser::parse_sql(&dialect, sql) {
            Ok(ast) => ast,
            Err(e) => {
                debug!("Failed to parse SQL: {}", e);
                return Err(format!("Failed to parse SQL: {}", e));
            }
        };

        if ast.len() != 1 {
            debug!("Error: Expected exactly one statement");
            return Err("Expected exactly one statement".to_string());
        }

        if self.log_detailed {
            debug!("Parsed AST: {:?}", ast[0]);
        }

        match &ast[0] {
            Statement::CreateTable(create_table) => {
                debug!("Planning CREATE TABLE statement");
                self.plan_create_table(create_table)
            }
            Statement::Query(query) => {
                debug!("Planning SELECT query");
                self.plan_query(query)
            }
            Statement::Insert(insert) => {
                debug!("Planning INSERT statement");
                self.plan_insert(insert)
            }
            _ => {
                debug!("Error: Unsupported statement type");
                Err("Only SELECT statements are supported".to_string())
            }
        }
    }

    fn plan_create_table(&mut self, create_table: &CreateTable) -> Result<PlanNode, String> {
        let columns = self.convert_column_defs(&create_table.columns)?;
        let schema = Schema::new(columns);
        let table_name = create_table.name.to_string();

        // Add the new table metadata to the catalog
        let mut catalog_guard = self.catalog.write();
        if let Some(_) = catalog_guard.create_table(&table_name, schema.clone()) {
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

        // Extract table name first
        let table_name = match &select.from[0].relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err("Only simple table scans are supported".to_string()),
        };

        // Get schema and table info from catalog
        let catalog_guard = self.catalog.read();
        let table_info = catalog_guard
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' not found in catalog", table_name))?;
        let schema = table_info.get_table_schema();
        let table_oid = table_info.get_table_oidt();

        // Plan the base table scan
        let table_scan = PlanNode::SeqScan(SeqScanPlanNode::new(
            schema.clone(),
            table_oid,
            table_name.clone(),
            None,
        ));

        // If there's a WHERE clause, add a filter node
        if let Some(where_clause) = &select.selection {
            let filter_expr = self.parse_expression(where_clause, &schema)?;

            Ok(PlanNode::Filter(FilterNode::new(
                schema.clone(),
                table_oid,
                table_name,
                filter_expr,
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
            Some(query) => match &*query.body {
                // Dereference the Box here
                SetExpr::Values(values) => self.plan_values_node(&values.rows, &table_schema)?,
                SetExpr::Select(select) => {
                    let select_plan = self.plan_select(select)?;
                    if !self.schemas_compatible(select_plan.get_output_schema(), &table_schema) {
                        return Err("SELECT schema does not match INSERT target schema".to_string());
                    }
                    select_plan
                }
                _ => {
                    return Err(
                        "Only VALUES and SELECT are supported in INSERT statements".to_string()
                    )
                }
            },
            None => {
                return Err("INSERT statement must have a source (VALUES or SELECT)".to_string())
            }
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
                .map(|(_column_index, expr)| {
                    self.parse_expression(expr, schema)
                        .map(|expr| Arc::new(expr))
                })
                .collect();

            all_rows.push(row_expressions?);
        }

        Ok(PlanNode::Values(
            ValuesNode::new(schema.clone(), all_rows, PlanNode::Empty).unwrap(),
        ))
    }

    fn plan_table_scan(&self, table: &TableWithJoins) -> Result<PlanNode, String> {
        match &table.relation {
            TableFactor::Table { name, .. } => {
                // Table name as a string
                let table_name = name.to_string();

                // Fetch the schema from the catalog
                let schema = self
                    .catalog
                    .read()
                    .get_table_schema(&table_name)
                    .ok_or_else(|| format!("Table '{}' not found in the catalog", table_name))?;

                // Use the real schema and include table_oid from the catalog
                let table_oid = self
                    .catalog
                    .read()
                    .get_table(&table_name)
                    .ok_or_else(|| {
                        format!("Table OID for '{}' not found in the catalog", table_name)
                    })?
                    .get_table_oidt();

                Ok(PlanNode::SeqScan(SeqScanPlanNode::new(
                    schema, table_oid, table_name, None,
                )))
            }
            _ => Err("Only simple table scans are supported".to_string()),
        }
    }

    fn parse_expression(&self, expr: &Expr, schema: &Schema) -> Result<Expression, String> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_expr = self.parse_expression(left, schema)?;
                let right_expr = self.parse_expression(right, schema)?;

                let comparison_type = match op {
                    sqlparser::ast::BinaryOperator::Eq => ComparisonType::Equal,
                    sqlparser::ast::BinaryOperator::Gt => ComparisonType::GreaterThan,
                    sqlparser::ast::BinaryOperator::Lt => ComparisonType::LessThan,
                    sqlparser::ast::BinaryOperator::GtEq => ComparisonType::GreaterThanOrEqual,
                    sqlparser::ast::BinaryOperator::LtEq => ComparisonType::LessThanOrEqual,
                    sqlparser::ast::BinaryOperator::NotEq => ComparisonType::NotEqual,
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
                    _ => return Err("Unsupported value type".to_string()),
                };

                Ok(Expression::Constant(ConstantExpression::new(
                    val,
                    Column::new("const", TypeId::Integer),
                    vec![],
                )))
            }
            Expr::Identifier(ident) => {
                // Look up column in the provided schema
                let column_idx = schema
                    .get_column_index(&ident.value)
                    .ok_or_else(|| format!("Column {} not found in schema", ident.value))?;

                let column = schema
                    .get_column(column_idx)
                    .ok_or_else(|| format!("Failed to get column at index {}", column_idx))?;

                Ok(Expression::ColumnRef(ColumnRefExpression::new(
                    0,
                    column_idx,
                    column.clone(),
                    vec![],
                )))
            }
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

    fn create_numeric_expression(
        &self,
        value: &str,
        column: &Column,
    ) -> Result<ConstantExpression, String> {
        match column.get_type() {
            TypeId::Integer => {
                let num = value
                    .parse::<i32>()
                    .map_err(|_| format!("Failed to parse {} as integer", value))?;
                Ok(ConstantExpression::new(
                    Value::new(num),
                    column.clone(),
                    Vec::new(),
                ))
            }
            _ => Err(format!("Cannot convert number to {:?}", column.get_type())),
        }
    }

    fn create_string_expression(
        &self,
        value: &str,
        column: &Column,
    ) -> Result<ConstantExpression, String> {
        if column.get_type() != TypeId::VarChar {
            return Err(format!(
                "Cannot insert string into column of type {:?}",
                column.get_type()
            ));
        }
        Ok(ConstantExpression::new(
            Value::new(value.to_string()),
            column.clone(),
            Vec::new(),
        ))
    }

    fn create_boolean_expression(
        &self,
        value: bool,
        column: &Column,
    ) -> Result<ConstantExpression, String> {
        if column.get_type() != TypeId::Boolean {
            return Err(format!(
                "Cannot insert boolean into column of type {:?}",
                column.get_type()
            ));
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
            sqlparser::ast::Value::Number(number, _) => {
                self.create_numeric_expression(number, column)
            }
            sqlparser::ast::Value::SingleQuotedString(string)
            | sqlparser::ast::Value::DoubleQuotedString(string) => {
                self.create_string_expression(string, column)
            }
            sqlparser::ast::Value::Boolean(boolean) => {
                self.create_boolean_expression(*boolean, column)
            }
            sqlparser::ast::Value::Null => self.create_null_expression(column),
            _ => Err(format!("Unsupported SQL value type: {:?}", value)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use std::collections::HashMap;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        planner: QueryPlanner,
        _db_file: String,
        _log_file: String,
        _disk_manager: Arc<FileDiskManager>,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 5;
            const K: usize = 2;

            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

            let disk_manager =
                Arc::new(FileDiskManager::new(db_file.clone(), log_file.clone(), 100));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                bpm,
                0,              // next_index_oid
                0,              // next_table_oid
                HashMap::new(), // tables
                HashMap::new(), // indexes
                HashMap::new(), // table_names
                HashMap::new(), // index_names
            )));

            let planner = QueryPlanner::new(Arc::clone(&catalog));

            Self {
                catalog,
                planner,
                _db_file: db_file,
                _log_file: log_file,
                _disk_manager: disk_manager,
            }
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self._db_file);
            let _ = std::fs::remove_file(&self._log_file);
        }
    }

    #[test]
    fn test_create_table_plan() {
        let mut ctx = TestContext::new("create_table_test");

        // Create a table
        let sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255), age INTEGER)";
        let plan = ctx.planner.create_plan(sql).unwrap();

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

                // Verify table was added to catalog
                let catalog = ctx.catalog.read();
                let table = catalog.get_table("users");
                assert!(table.is_some());
                assert_eq!(table.unwrap().get_table_schema(), schema.clone());
            }
            _ => panic!("Expected CreateTable plan node"),
        }
    }

    #[test]
    fn test_select_queries() {
        let mut ctx = TestContext::new("select_queries_test");

        // First create a table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255), age INTEGER)";
        ctx.planner.create_plan(create_sql).unwrap();

        // Test simple select
        let select_sql = "SELECT * FROM users";
        let plan = ctx.planner.create_plan(select_sql).unwrap();

        match plan {
            PlanNode::SeqScan(scan) => {
                assert_eq!(scan.get_table_name(), "users");
                assert_eq!(scan.get_output_schema().get_column_count(), 3);
            }
            _ => panic!("Expected SeqScan plan node"),
        }

        // Test select with filter
        let filter_sql = "SELECT * FROM users WHERE age > 25";
        let plan = ctx.planner.create_plan(filter_sql).unwrap();

        match plan {
            PlanNode::Filter(filter) => {
                assert_eq!(filter.get_table_name(), "users");
                match filter.get_child_plan() {
                    PlanNode::SeqScan(scan) => {
                        assert_eq!(scan.get_table_name(), "users");
                        assert_eq!(scan.get_output_schema().get_column_count(), 3);
                    }
                    _ => panic!("Expected SeqScan as child of Filter"),
                }
            }
            _ => panic!("Expected Filter plan node"),
        }
    }

    #[test]
    fn test_insert_plan() {
        let mut ctx = TestContext::new("insert_test");

        // Create table first
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255))";
        ctx.planner.create_plan(create_sql).unwrap();

        // Test insert
        let insert_sql = "INSERT INTO users VALUES (1, 'test')";
        let plan = ctx.planner.create_plan(insert_sql).unwrap();

        match plan {
            PlanNode::Insert(insert) => {
                assert_eq!(insert.get_table_name(), "users");

                // Check child plan (Values node)
                match insert.get_child() {
                    // Double dereference to get to the inner PlanNode
                    PlanNode::Values(values) => {
                        assert_eq!(values.get_output_schema().get_column_count(), 2);
                    }
                    _ => panic!("Expected Values node as child of Insert"),
                }

                // Verify schema matches table schema
                let catalog = ctx.catalog.read();
                let table_schema = catalog.get_table_schema("users").unwrap();
                assert_eq!(insert.get_output_schema(), &table_schema);
            }
            _ => panic!("Expected Insert plan node"),
        }
    }

    #[test]
    fn test_error_handling() {
        let mut ctx = TestContext::new("error_handling_test");

        // Test invalid SQL
        let invalid_sql = "INVALID SQL";
        assert!(ctx.planner.create_plan(invalid_sql).is_err());

        // Test selecting from non-existent table
        let select_sql = "SELECT * FROM nonexistent_table";
        assert!(ctx.planner.create_plan(select_sql).is_err());

        // Test creating duplicate table
        let create_sql = "CREATE TABLE users (id INTEGER)";
        ctx.planner.create_plan(create_sql).unwrap();
        assert!(ctx.planner.create_plan(create_sql).is_err());

        // Test insert into non-existent table
        let insert_sql = "INSERT INTO nonexistent_table VALUES (1)";
        assert!(ctx.planner.create_plan(insert_sql).is_err());
    }

    #[test]
    fn test_select_with_where() {
        let mut ctx = TestContext::new("select_where_test");

        // First create a table
        let create_sql = "CREATE TABLE users (id INTEGER, name VARCHAR(255), age INTEGER)";
        ctx.planner.create_plan(create_sql).unwrap();

        // Test select with different comparison operators
        let test_cases = vec![
            (
                "SELECT * FROM users WHERE age > 25",
                ComparisonType::GreaterThan,
            ),
            ("SELECT * FROM users WHERE age = 30", ComparisonType::Equal),
            (
                "SELECT * FROM users WHERE age < 40",
                ComparisonType::LessThan,
            ),
            (
                "SELECT * FROM users WHERE age >= 25",
                ComparisonType::GreaterThanOrEqual,
            ),
            (
                "SELECT * FROM users WHERE age <= 40",
                ComparisonType::LessThanOrEqual,
            ),
            (
                "SELECT * FROM users WHERE age != 35",
                ComparisonType::NotEqual,
            ),
        ];

        for (sql, expected_comp_type) in test_cases {
            let plan = ctx.planner.create_plan(sql).unwrap();

            match plan {
                PlanNode::Filter(filter) => {
                    assert_eq!(filter.get_table_name(), "users");

                    // Verify predicate
                    match filter.get_filter_predicate() {
                        Expression::Comparison(comp) => {
                            assert_eq!(comp.get_comp_type(), expected_comp_type);

                            // Verify left side is column reference
                            match comp.get_left().as_ref() {
                                Expression::ColumnRef(col_ref) => {
                                    assert_eq!(col_ref.get_return_type().to_string(true), "age");
                                }
                                _ => panic!("Expected ColumnRef on left side of comparison"),
                            }
                        }
                        _ => panic!("Expected Comparison expression"),
                    }

                    // Verify child plan
                    match filter.get_child_plan() {
                        PlanNode::SeqScan(scan) => {
                            assert_eq!(scan.get_table_name(), "users");
                            assert_eq!(scan.get_output_schema().get_column_count(), 3);
                        }
                        _ => panic!("Expected SeqScan as child of Filter"),
                    }
                }
                _ => panic!("Expected Filter plan node"),
            }
        }
    }
}
