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
                table,
                assignments,
                from,
                selection,
                returning,
                or,
            } => self.plan_builder.build_update_plan(
                table,
                assignments,
                from,
                selection,
                returning,
                or,
            ),
            Statement::Delete(_) => self.plan_builder.build_delete_plan(stmt),
            Statement::Explain { .. } => self.plan_builder.build_explain_plan(stmt),
            Statement::StartTransaction {
                modes,
                begin,
                transaction,
                modifier,
                statements,
                exception_statements,
                has_end_keyword,
            } => self.plan_builder.build_start_transaction_plan(
                modes,
                begin,
                transaction,
                modifier,
                statements,
                exception_statements,
                has_end_keyword,
            ),
            Statement::Commit {
                chain,
                modifier,
                end,
            } => self.plan_builder.build_commit_plan(chain, end, modifier),
            Statement::Rollback { chain, savepoint } => {
                self.plan_builder.build_rollback_plan(chain, savepoint)
            }
            Statement::Savepoint { name } => self.plan_builder.build_savepoint_plan(name),
            Statement::ReleaseSavepoint { name } => {
                self.plan_builder.build_release_savepoint_plan(name)
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                ..
            } => self
                .plan_builder
                .build_create_schema_plan(schema_name, if_not_exists),
            Statement::CreateDatabase {
                db_name,
                if_not_exists,
                location,
                managed_location,
            } => self.plan_builder.build_create_database_plan(
                db_name,
                if_not_exists,
                location,
                managed_location,
            ),
            Statement::AlterTable {
                name,
                if_exists,
                only,
                operations,
                location,
                on_cluster,
            } => self
                .plan_builder
                .build_alter_table_plan(name, if_exists, only, operations, location, on_cluster),
            Statement::CreateView {
                or_alter,
                or_replace,
                materialized,
                name,
                columns,
                query,
                options,
                cluster_by,
                comment,
                with_no_schema_binding,
                if_not_exists,
                temporary,
                to,
                params,
            } => self.plan_builder.build_create_view_plan(
                or_alter,
                or_replace,
                materialized,
                name,
                columns,
                query,
                options,
                cluster_by,
                comment,
                with_no_schema_binding,
                if_not_exists,
                temporary,
                to,
                params,
            ),
            Statement::AlterView {
                name,
                columns,
                query,
                with_options,
            } => self
                .plan_builder
                .build_alter_view_plan(name, columns, query, with_options),
            Statement::ShowTables {
                terse,
                history,
                extended,
                full,
                external,
                show_options,
            } => self.plan_builder.build_show_tables_plan(
                terse,
                history,
                extended,
                full,
                external,
                show_options,
            ),
            Statement::ShowDatabases {
                terse,
                history,
                show_options,
            } => self
                .plan_builder
                .build_show_databases_plan(terse, history, show_options),
            Statement::ShowColumns {
                extended,
                full,
                show_options,
            } => self
                .plan_builder
                .build_show_columns_plan(extended, full, show_options),
            Statement::Use(stmt) => self.plan_builder.build_use_plan(stmt),
            _ => Err(format!("Unsupported statement type: {:?}", stmt)),
        }
    }

    pub fn explain(&mut self, sql: &str) -> Result<String, String> {
        let plan = self.create_logical_plan(sql)?;
        Ok(format!("Query Plan:\n{}\n", plan.explain(0)))
    }
}
