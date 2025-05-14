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
            _ => Err(format!("Unsupported statement type: {:?}", stmt)),
        }
    }

    pub fn explain(&mut self, sql: &str) -> Result<String, String> {
        let plan = self.create_logical_plan(sql)?;
        Ok(format!("Query Plan:\n{}\n", plan.explain(0)))
    }
}
