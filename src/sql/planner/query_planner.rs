use super::logical_plan::LogicalPlan;
use crate::catalog::catalog::Catalog;
use crate::sql::planner::plan_builder::LogicalPlanBuilder;
use parking_lot::RwLock;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

/// 4. Orchestrates the planning process
pub struct QueryPlanner {
    plan_builder: LogicalPlanBuilder,
}

impl QueryPlanner {
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            plan_builder: LogicalPlanBuilder::new(Arc::clone(&catalog)),
        }
    }

    pub fn create_logical_plan(&mut self, sql: &str) -> Result<Box<LogicalPlan>, String> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

        if ast.len() != 1 {
            return Err("Only single SQL statement is supported".to_string());
        }

        self.create_logical_plan_from_statement(&ast[0])
    }

    pub fn create_logical_plan_from_statement(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<LogicalPlan>, String> {
        match stmt {
            Statement::Query(query) => self.plan_builder.build_query_plan(query),
            Statement::Insert(stmt) => self.plan_builder.build_insert_plan(stmt),
            Statement::CreateTable(stmt) => self.plan_builder.build_create_table_plan(stmt),
            Statement::CreateIndex(stmt) => self.plan_builder.build_create_index_plan(stmt),
            Statement::Update {
                table: _,
                assignments: _,
                from: _,
                selection: _,
                returning: _,
                or: _,
            } => self.plan_builder.build_update_plan(stmt),
            Statement::Delete(_) => self.plan_builder.build_delete_plan(stmt),
            Statement::Explain { .. } => self.plan_builder.build_explain_plan(stmt),
            Statement::StartTransaction {
                modes: _,
                begin: _,
                transaction: _,
                modifier: _,
                statements: _,
                exception_statements: _,
                has_end_keyword: _
            } => self.plan_builder.build_start_transaction_plan(stmt),
            Statement::Commit {
                chain: _,
                modifier: _,
                end: _,
            } => self.plan_builder.build_commit_plan(stmt),
            Statement::Rollback {
                chain: _,
                savepoint: _,
            } => self.plan_builder.build_rollback_plan(stmt),
            Statement::Savepoint {
                name
            } => self.plan_builder.build_savepoint_plan(stmt),
            Statement::ReleaseSavepoint {
                name
            } => self.plan_builder.build_release_savepoint_plan(stmt),
            Statement::CreateSchema {
                schema_name: _, 
                if_not_exists: _, 
                options: _, 
                default_collate_spec: _
            } => self.plan_builder.build_create_schema_plan(stmt),
            Statement::CreateDatabase {
                db_name:_, 
                if_not_exists: _, 
                location:_, 
                managed_location:_
            } => self.plan_builder.build_create_database_plan(stmt),
            Statement::AlterTable {
                name: _, 
                if_exists: _, 
                only: _, 
                operations: _, 
                location: _, 
                on_cluster: _
            } => self.plan_builder.build_alter_table_plan(stmt),
            Statement::CreateView {
                or_alter: _, 
                or_replace: _, 
                materialized: _, 
                name: _, 
                columns: _, 
                query: _, 
                options: _, 
                cluster_by: _, 
                comment: _, 
                with_no_schema_binding: _, 
                if_not_exists: _, 
                temporary: _, 
                to: _, 
                params: _
            } => self.plan_builder.build_create_view_plan(stmt),
            Statement::AlterView {
                name: _, 
                columns: _, 
                query: _, 
                with_options: _
            } => self.plan_builder.build_alter_view_plan(stmt),
            Statement::ShowTables {
                terse: _, 
                history: _, 
                extended: _, 
                full: _, 
                external: _, 
                show_options: _
            } => self.plan_builder.build_show_tables_plan(stmt),
            Statement::ShowDatabases {
                terse: _, 
                history: _, 
                show_options: _
            } => self.plan_builder.build_show_databases_plan(stmt),
            Statement::ShowColumns {
                extended: _, 
                full: _, 
                show_options: _
            } => self.plan_builder.build_show_columns_plan(stmt),
            Statement::Use(stmt) => self.plan_builder.build_use_plan(stmt),
            _ => Err(format!("Unsupported statement type: {:?}", stmt)),
        }
    }

    pub fn explain(&mut self, sql: &str) -> Result<String, String> {
        let plan = self.create_logical_plan(sql)?;
        Ok(format!("Query Plan:\n{}\n", plan.explain(0)))
    }
}
