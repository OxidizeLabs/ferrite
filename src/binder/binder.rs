use crate::binder::bound_expression::BoundExpression;
use crate::binder::bound_order_by::{BoundOrderBy, OrderByType};
use crate::binder::bound_statement::{AnyBoundStatement, BoundStatement};
use crate::binder::bound_table_ref::BoundTableRef;
use crate::binder::expressions::bound_alias::BoundAlias;
use crate::binder::expressions::bound_binary_op::BoundBinaryOp;
use crate::binder::expressions::bound_column_ref::BoundColumnRef;
use crate::binder::expressions::bound_constant::BoundConstant;
use crate::binder::expressions::bound_star::BoundStar;
use crate::binder::expressions::bound_window::BoundWindow;
use crate::binder::simplified_tokens::{ParserKeyword, SimplifiedToken};
use crate::binder::statement::create_statement::CreateStatement;
use crate::binder::statement::explain_statement::ExplainStatement;
use crate::binder::statement::select_statement::SelectStatement;
use crate::binder::table_ref::bound_base_table_ref::BoundBaseTableRef;
use crate::binder::table_ref::bound_subquery_ref::{BoundSubqueryRef, CTEList};
use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::LRUKReplacer;
use crate::catalogue::catalogue::Catalog;
use crate::catalogue::column::Column;
use crate::common::logger::initialize_logger;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::recovery::log_manager::LogManager;
use crate::storage::disk::disk_manager::FileDiskManager;
use crate::storage::disk::disk_scheduler::DiskScheduler;
use crate::types_db::value::Value as DbValue;
use chrono::Utc;
use parking_lot::RwLock;
use sqlparser::ast::Value as SqlValue;
use sqlparser::ast::{
    ColumnDef, Expr, Function, GroupByExpr, GroupByWithModifier, Ident, Join, Offset, OrderByExpr,
    Query, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value, WindowSpec, With,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::fs;
use std::sync::Arc;

pub struct Binder {
    catalog: Arc<Catalog>,
    scope: Option<Box<dyn BoundTableRef>>,
    cte_scope: Option<Vec<Box<dyn BoundTableRef>>>,
    universal_id: usize,
}

impl Binder {
    pub fn new(catalog: Arc<Catalog>) -> Self {
        Self {
            catalog,
            scope: None,
            cte_scope: None,
            universal_id: 0,
        }
    }

    pub fn parse_and_bind(
        &mut self,
        query: &str,
    ) -> Result<Vec<Box<dyn AnyBoundStatement>>, String> {
        let dialect = GenericDialect {};
        let ast = match Parser::parse_sql(&dialect, query) {
            Ok(ast) => ast,
            Err(e) => return Err(format!("SQL parsing error: {}", e)),
        };

        ast.iter()
            .map(|stmt| {
                self.bind_statement(stmt)
                    .map_err(|e| format!("Statement binding error: {}", e))
            })
            .collect()
    }

    pub fn bind_statement(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<dyn AnyBoundStatement>, String> {
        match stmt {
            Statement::Query(query) => Ok(Box::new(self.bind_select(query)?)),
            Statement::Insert { .. } => Ok(Box::new(self.bind_insert(stmt)?)),
            Statement::Update { .. } => Ok(Box::new(self.bind_update(stmt)?)),
            Statement::Delete { .. } => Ok(Box::new(self.bind_delete(stmt)?)),
            Statement::CreateTable { .. } => Ok(Box::new(self.bind_create_table(stmt)?)),
            // Add other statement types as needed
            _ => Err(format!("Unsupported statement type: {:?}", stmt)),
        }
    }

    pub fn is_keyword(text: &str) -> bool {
        // Implementation depends on sqlparser's keyword list
        unimplemented!()
    }

    pub fn keyword_list() -> Vec<ParserKeyword> {
        // Implementation depends on sqlparser's keyword list
        unimplemented!()
    }

    pub fn tokenize(query: &str) -> Vec<SimplifiedToken> {
        // This would need to be implemented using sqlparser's lexer
        unimplemented!()
    }

    pub fn bind_explain(
        &mut self,
        stmt: &ExplainStatement,
    ) -> Result<Box<dyn BoundStatement>, String> {
        // Implementation here
        unimplemented!("bind_explain not implemented")
    }

    pub fn bind_create(
        &mut self,
        stmt: &CreateStatement,
    ) -> Result<Box<dyn BoundStatement>, String> {
        // Implementation here
        unimplemented!("bind_create not implemented")
    }

    pub fn bind_column_definition(&mut self, cdef: &ColumnDef) -> Result<Column, String> {
        // Implementation here
        unimplemented!("bind_column_definition not implemented")
    }

    pub fn bind_subquery(
        &mut self,
        node: &Query,
        alias: &str,
    ) -> Result<Box<BoundSubqueryRef>, String> {
        // Implementation here
        unimplemented!("bind_subquery not implemented")
    }

    pub fn bind_where(
        &mut self,
        root: &Option<Expr>,
    ) -> Result<Option<Box<dyn BoundExpression>>, String> {
        // Implementation here
        unimplemented!("bind_where not implemented")
    }

    pub fn bind_group_by(
        &mut self,
        list: &[Expr],
    ) -> Result<Vec<Box<dyn BoundExpression>>, String> {
        // Implementation here
        unimplemented!("bind_group_by not implemented")
    }

    pub fn bind_having(
        &mut self,
        root: &Option<Expr>,
    ) -> Result<Option<Box<dyn BoundExpression>>, String> {
        // Implementation here
        unimplemented!("bind_having not implemented")
    }

    pub fn bind_expression_list(
        &mut self,
        list: &[Expr],
    ) -> Result<Vec<Box<dyn BoundExpression>>, String> {
        // Implementation here
        unimplemented!("bind_expression_list not implemented")
    }

    pub fn bind_constant(&mut self, node: &Value) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("bind_constant not implemented")
    }

    pub fn bind_column_ref(&mut self, node: &Ident) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("bind_column_ref not implemented")
    }

    pub fn bind_res_target(
        &mut self,
        root: &SelectItem,
    ) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("bind_res_target not implemented")
    }

    pub fn bind_star(&mut self) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("bind_star not implemented")
    }

    pub fn bind_func_call(&mut self, root: &Function) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("bind_func_call not implemented")
    }

    pub fn bind_window_frame(
        &mut self,
        window_spec: &WindowSpec,
        expr: Box<BoundWindow>,
    ) -> Result<Box<BoundWindow>, String> {
        // Implementation here
        unimplemented!("bind_window_frame not implemented")
    }

    pub fn bind_window_expression(
        &mut self,
        func_name: &str,
        children: Vec<Box<dyn BoundExpression>>,
        node: &WindowSpec,
    ) -> Result<Box<BoundWindow>, String> {
        // Implementation here
        unimplemented!("bind_window_expression not implemented")
    }

    pub fn bind_a_expr(&mut self, root: &Expr) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("bind_a_expr not implemented")
    }

    pub fn bind_bool_expr(&mut self, root: &Expr) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("bind_bool_expr not implemented")
    }

    pub fn bind_range_var(
        &mut self,
        table_ref: &TableFactor,
    ) -> Result<Box<dyn BoundTableRef>, String> {
        // Implementation here
        unimplemented!("bind_range_var not implemented")
    }

    pub fn bind_table_ref(&mut self, node: &TableFactor) -> Result<Box<dyn BoundTableRef>, String> {
        // Implementation here
        unimplemented!("bind_table_ref not implemented")
    }

    pub fn bind_join(&mut self, root: &Join) -> Result<Box<dyn BoundTableRef>, String> {
        // Implementation here
        unimplemented!("bind_join not implemented")
    }

    pub fn get_all_columns(
        &self,
        scope: &dyn BoundTableRef,
    ) -> Result<Vec<Box<dyn BoundExpression>>, String> {
        // Implementation here
        unimplemented!("get_all_columns not implemented")
    }

    pub fn resolve_column(
        &self,
        scope: &dyn BoundTableRef,
        col_name: &[String],
    ) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("resolve_column not implemented")
    }

    pub fn resolve_column_internal(
        &self,
        table_ref: &dyn BoundTableRef,
        col_name: &[String],
    ) -> Result<Box<dyn BoundExpression>, String> {
        // Implementation here
        unimplemented!("resolve_column_internal not implemented")
    }

    pub fn resolve_column_ref_from_select_list(
        &self,
        subquery_select_list: &[Vec<String>],
        col_name: &[String],
    ) -> Result<Box<BoundColumnRef>, String> {
        // Implementation here
        unimplemented!("resolve_column_ref_from_select_list not implemented")
    }

    pub fn resolve_column_ref_from_base_table_ref(
        &self,
        table_ref: &BoundBaseTableRef,
        col_name: &[String],
    ) -> Result<Box<BoundColumnRef>, String> {
        // Implementation here
        unimplemented!("resolve_column_ref_from_base_table_ref not implemented")
    }

    pub fn resolve_column_ref_from_subquery_ref(
        &self,
        subquery_ref: &BoundSubqueryRef,
        alias: &str,
        col_name: &[String],
    ) -> Result<Box<BoundColumnRef>, String> {
        // Implementation here
        unimplemented!("resolve_column_ref_from_subquery_ref not implemented")
    }

    pub fn bind_limit_count(
        &mut self,
        root: &Option<Expr>,
    ) -> Result<Option<Box<dyn BoundExpression>>, String> {
        // Implementation here
        unimplemented!("bind_limit_count not implemented")
    }

    pub fn bind_limit_offset(
        &mut self,
        root: &Option<Expr>,
    ) -> Result<Option<Box<dyn BoundExpression>>, String> {
        // Implementation here
        unimplemented!("bind_limit_offset not implemented")
    }

    pub fn bind_sort(&mut self, list: &[OrderByExpr]) -> Result<Vec<Box<BoundOrderBy>>, String> {
        // Implementation here
        unimplemented!("bind_sort not implemented")
    }

    pub fn bind_index(&mut self, stmt: &Statement) -> Result<Box<dyn BoundStatement>, String> {
        // Implementation here
        unimplemented!("bind_index not implemented")
    }

    pub fn bind_cte(&mut self, node: &Option<With>) -> Result<Vec<Box<BoundSubqueryRef>>, String> {
        // Implementation here
        unimplemented!("bind_cte not implemented")
    }

    pub fn bind_variable_set(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<dyn BoundStatement>, String> {
        // Implementation here
        unimplemented!("bind_variable_set not implemented")
    }

    pub fn bind_variable_show(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<dyn BoundStatement>, String> {
        // Implementation here
        unimplemented!("bind_variable_show not implemented")
    }

    pub fn bind_transaction(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<dyn BoundStatement>, String> {
        // Implementation here
        unimplemented!("bind_transaction not implemented")
    }

    pub fn bind_select(&mut self, query: &Query) -> Result<Box<dyn BoundStatement>, String> {
        let select = match &*query.body {
            SetExpr::Select(box_select) => box_select,
            _ => return Err("Expected SELECT statement".to_string()),
        };

        // Bind the FROM clause
        let table = self.bind_from(&select.from)?;

        // Bind the SELECT list
        let select_list = self.bind_select_list(&select.projection)?;

        // Bind the WHERE clause
        let where_clause = select
            .selection
            .as_ref()
            .map(|expr| self.bind_expression(expr))
            .transpose()?;

        // Bind the GROUP BY clause
        let group_by = match &select.group_by {
            GroupByExpr::All(modifiers) => self.bind_group_by_all(modifiers)?,
            GroupByExpr::Expressions(exprs, ..) => exprs
                .iter()
                .map(|expr| self.bind_expression(expr))
                .collect::<Result<Vec<_>, _>>()?,
        };

        // Bind the HAVING clause
        let having = select
            .having
            .as_ref()
            .map(|expr| self.bind_expression(expr))
            .transpose()?;

        // Bind the ORDER BY clause
        let sort = match &query.order_by {
            Some(order_by) => self.bind_order_by(&order_by.exprs)?,
            None => vec![],
        };

        // Bind the LIMIT and OFFSET
        let (limit_count, limit_offset) = match &query.limit {
            Some(limit) => {
                let count = self.bind_expression(limit)?;
                let offset = match &query.offset {
                    Some(offset_expr) => Some(self.bind_offset(offset_expr)?),
                    None => None,
                };
                (Some(count), offset)
            }
            None => (None, None),
        };

        // Bind CTEs (Common Table Expressions)
        let ctes = self.bind_ctes(&query.with)?;

        // Create the SelectStatement
        let stmt = SelectStatement::new(
            table,
            select_list,
            where_clause,
            group_by,
            having,
            limit_count,
            limit_offset,
            sort,
            ctes,
            select.distinct.is_some(),
        );

        Ok(Box::new(stmt))
    }

    pub fn bind_insert(&mut self, stmt: &Statement) -> Result<Box<dyn BoundStatement>, String> {
        // Implement insert binding logic
        unimplemented!()
    }

    pub fn bind_update(&mut self, stmt: &Statement) -> Result<Box<dyn BoundStatement>, String> {
        // Implement update binding logic
        unimplemented!()
    }

    pub fn bind_delete(&mut self, stmt: &Statement) -> Result<Box<dyn BoundStatement>, String> {
        // Implement delete binding logic
        unimplemented!()
    }

    pub fn bind_create_table(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<dyn BoundStatement>, String> {
        // Implement create table binding logic
        unimplemented!()
    }

    // Helper functions (you'll need to implement these)

    fn bind_from(&mut self, from: &[TableWithJoins]) -> Result<Box<dyn BoundTableRef>, String> {
        if from.is_empty() {
            return Err("FROM clause is empty".to_string());
        }
        // For simplicity, we'll just bind the first table. In a full implementation,
        // you'd need to handle joins as well.
        let table = &from[0].relation;
        match table {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let alias = alias.as_ref().map(|a| a.to_string());
                self.bind_base_table_ref(table_name, alias)
            }
            // Handle other types of TableFactor (subquery, derived table, etc.)
            _ => Err("Unsupported table type in FROM clause".to_string()),
        }
    }

    fn bind_select_list(
        &mut self,
        projection: &[SelectItem],
    ) -> Result<Vec<Box<dyn BoundExpression>>, String> {
        projection
            .iter()
            .map(|item| self.bind_select_item(item))
            .collect()
    }

    fn bind_select_item(&mut self, item: &SelectItem) -> Result<Box<dyn BoundExpression>, String> {
        match item {
            SelectItem::UnnamedExpr(expr) => self.bind_expression(expr),
            SelectItem::ExprWithAlias { expr, alias } => {
                let bound_expr = self.bind_expression(expr)?;
                Ok(Box::new(BoundAlias::new(alias.to_string(), bound_expr)))
            }
            SelectItem::Wildcard(expr) => Ok(Box::new(BoundStar::new())),
            // Handle other types of SelectItem
            _ => Err("Unsupported select item type".to_string()),
        }
    }

    fn bind_expression(&mut self, expr: &Expr) -> Result<Box<dyn BoundExpression>, String> {
        match expr {
            Expr::Identifier(ident) => Ok(Box::new(BoundColumnRef::new(vec![ident.value.clone()]))),
            Expr::CompoundIdentifier(idents) => {
                let col_name = idents.iter().map(|i| i.value.clone()).collect();
                Ok(Box::new(BoundColumnRef::new(col_name)))
            }
            Expr::Value(value) => {
                let db_value = self.sql_value_to_db_value(value)?;
                Ok(Box::new(BoundConstant::new(db_value)))
            }
            Expr::BinaryOp { left, op, right } => {
                let left = self.bind_expression(left)?;
                let right = self.bind_expression(right)?;
                Ok(Box::new(BoundBinaryOp::new(op, left, right)))
            }
            // Handle other types of expressions
            _ => Err(format!("Unsupported expression type: {:?}", expr)),
        }
    }

    fn sql_value_to_db_value(&self, value: &SqlValue) -> Result<DbValue, String> {
        match value {
            SqlValue::Number(n, _) => {
                // Attempt to parse as different numeric types
                if let Ok(i) = n.parse::<i32>() {
                    Ok(DbValue::from(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(DbValue::from(f))
                } else {
                    Err(format!("Unable to parse number: {}", n))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(DbValue::from(s.clone()))
            }
            SqlValue::Boolean(b) => Ok(DbValue::from(*b)),
            SqlValue::Null => Err("Null values are not supported".to_string()),
            // Handle other types as needed
            _ => Err(format!("Unsupported SQL value type: {:?}", value)),
        }
    }

    fn bind_offset(&mut self, offset: &Offset) -> Result<Box<dyn BoundExpression>, String> {
        // Extract the expression from the Offset and bind it
        self.bind_expression(&offset.value)
    }

    fn bind_base_table_ref(
        &mut self,
        table_name: String,
        alias: Option<String>,
    ) -> Result<Box<dyn BoundTableRef>, String> {
        // Look up the table in the catalog
        let table_info = self
            .catalog
            .get_table(&table_name)
            .ok_or_else(|| format!("Table '{}' not found in catalog", table_name))?;

        let table_oid = table_info.get_table_oidt();

        let schema = table_info.get_table_schema();

        // Create a BoundBaseTableRef
        let bound_table = BoundBaseTableRef::new(table_name, table_oid, alias, schema);

        Ok(Box::new(bound_table))
    }

    fn bind_group_by_all(
        &mut self,
        modifiers: &[GroupByWithModifier],
    ) -> Result<Vec<Box<dyn BoundExpression>>, String> {
        let mut bound_exprs = Vec::new();

        for modifier in modifiers {
            match modifier {
                GroupByWithModifier::Rollup => {
                    todo!()
                }
                GroupByWithModifier::Cube => {
                    todo!()
                }
                GroupByWithModifier::Totals => {
                    todo!()
                }
            }
        }

        Ok(bound_exprs)
    }

    fn bind_order_by(
        &mut self,
        order_by: &[OrderByExpr],
    ) -> Result<Vec<Box<BoundOrderBy>>, String> {
        order_by
            .iter()
            .map(|expr| self.bind_single_order_by(expr))
            .collect()
    }

    fn bind_single_order_by(
        &mut self,
        order_by_expr: &OrderByExpr,
    ) -> Result<Box<BoundOrderBy>, String> {
        let bound_expr = self.bind_expression(&order_by_expr.expr)?;
        let order_type = if order_by_expr.asc.unwrap_or(true) {
            OrderByType::Asc
        } else {
            OrderByType::Desc
        };

        Ok(Box::new(BoundOrderBy::new(order_type, bound_expr)))
    }

    fn bind_ctes(&mut self, with: &Option<With>) -> Result<CTEList, String> {
        match with {
            Some(with) => with
                .cte_tables
                .iter()
                .map(|cte| {
                    let bound_stmt = self.bind_select(&cte.query)?;

                    let subquery = bound_stmt
                        .as_any()
                        .downcast_ref::<SelectStatement>()
                        .ok_or_else(|| "Expected a SELECT statement for CTE".to_string())?;

                    Ok(Box::new(BoundSubqueryRef::new(
                        Box::new(subquery.clone()),
                        vec![],
                        cte.alias.to_string(),
                    )))
                })
                .collect(),
            None => Ok(vec![]),
        }
    }

    fn bind_table_reference(
        &mut self,
        table_ref: &TableWithJoins,
    ) -> Result<Box<dyn BoundTableRef>, String> {
        // Implement table reference binding logic
        unimplemented!()
    }

    fn new_context(&mut self) -> ContextGuard {
        ContextGuard::new(&mut self.scope, &mut self.cte_scope)
    }
}

struct ContextGuard<'a> {
    old_scope: Option<Box<dyn BoundTableRef>>,
    scope: &'a mut Option<Box<dyn BoundTableRef>>,
    old_cte_scope: Option<Vec<Box<dyn BoundTableRef>>>,
    cte_scope: &'a mut Option<Vec<Box<dyn BoundTableRef>>>,
}

impl<'a> ContextGuard<'a> {
    fn new(
        scope: &'a mut Option<Box<dyn BoundTableRef>>,
        cte_scope: &'a mut Option<Vec<Box<dyn BoundTableRef>>>,
    ) -> Self {
        let old_scope = scope.take();
        let old_cte_scope = cte_scope.take();
        Self {
            old_scope,
            scope,
            old_cte_scope,
            cte_scope,
        }
    }
}

impl<'a> Drop for ContextGuard<'a> {
    fn drop(&mut self) {
        *self.scope = self.old_scope.take();
        *self.cte_scope = self.old_cte_scope.take();
    }
}

pub struct TestContext {
    bpm: Arc<BufferPoolManager>,
    transaction_manager: Arc<TransactionManager>,
    lock_manager: Arc<LockManager>,
    log_manager: Arc<LogManager>,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    pub fn new(test_name: &str) -> Self {
        initialize_logger();
        const BUFFER_POOL_SIZE: usize = 5;
        const K: usize = 2;

        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
        let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);

        let disk_manager = Arc::new(FileDiskManager::new(
            db_file.clone(),
            db_log_file.clone(),
            100,
        ));
        let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
        let bpm = Arc::new(BufferPoolManager::new(
            BUFFER_POOL_SIZE,
            disk_scheduler,
            disk_manager.clone(),
            replacer.clone(),
        ));
        let log_manager = Arc::new(LogManager::new(disk_manager.clone()));
        let catalog = Catalog::new(
            bpm.clone(),
            log_manager,
            0,
            0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        );

        // Create TransactionManager with a placeholder Catalog
        let transaction_manager = Arc::new(TransactionManager::new(Arc::from(catalog)));
        let lock_manager = Arc::new(LockManager::new(Arc::clone(&transaction_manager)));
        let log_manager = Arc::new(LogManager::new(Arc::clone(&disk_manager)));

        Self {
            bpm,
            transaction_manager,
            lock_manager,
            log_manager,
            db_file,
            db_log_file,
        }
    }

    pub fn bpm(&self) -> Arc<BufferPoolManager> {
        Arc::clone(&self.bpm)
    }

    pub fn lock_manager(&self) -> Arc<LockManager> {
        Arc::clone(&self.lock_manager)
    }

    pub fn log_manager(&self) -> Arc<LogManager> {
        Arc::clone(&self.log_manager)
    }

    fn cleanup(&self) {
        let _ = fs::remove_file(&self.db_file);
        let _ = fs::remove_file(&self.db_log_file);
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        self.cleanup();
    }
}

#[cfg(test)]
mod unit_tests {
    use log::info;
    use crate::catalogue::schema::Schema;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::types_db::type_id::TypeId;
    use super::*;

    #[test]
    fn parse_and_bind_select() {
        let ctx = TestContext::new("parse_and_bind_select");
        let bpm = ctx.bpm();
        let lock_manager = ctx.lock_manager();
        let log_manager = ctx.log_manager();

        // Create the real Catalog
        let txn = Transaction::new(0, IsolationLevel::Serializable);
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let mut catalog = Catalog::new(
            bpm,
            log_manager,
            0,
            0,
            Default::default(),
            Default::default(),
            Default::default(),
            Default::default(),
        );
        catalog.create_table(&txn, "users", schema, true);

        // Now we can create the Binder with the Catalog
        let mut binder = Binder::new(Arc::new(catalog));

        // Test the binder
        let result = binder.parse_and_bind("SELECT * FROM users");

        if let Err(e) = &result {
            info!("Error: {}", e);
        }

        assert!(result.is_ok(), "Expected Ok result, got Err");

        // Add more assertions based on expected behavior
        if let Ok(bound_statements) = result {
            assert_eq!(bound_statements.len(), 1);
            // Add more specific assertions about the bound statement
        }
    }
}
