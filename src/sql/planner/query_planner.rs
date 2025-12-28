//! # Query Planner
//!
//! This module provides the `QueryPlanner`, the entry point for transforming SQL text
//! into executable logical plans. It orchestrates the parsing and planning phases,
//! delegating the actual plan construction to the `LogicalPlanBuilder`.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────┐
//! │                              Query Processing Pipeline                          │
//! ├─────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                 │
//! │   ┌───────────────┐     ┌───────────────┐     ┌───────────────────────────┐     │
//! │   │   SQL Text    │ ──▶ │  sqlparser    │ ──▶ │       AST (Statement)     │     │
//! │   │ "SELECT ..."  │     │  Parser       │     │  Query { body, order... } │     │
//! │   └───────────────┘     └───────────────┘     └─────────────┬─────────────┘     │
//! │                                                             │                   │
//! │                                                             ▼                   │
//! │                                           ┌─────────────────────────────────┐   │
//! │                                           │         QueryPlanner            │   │
//! │                                           │  ┌───────────────────────────┐  │   │
//! │                                           │  │   LogicalPlanBuilder      │  │   │
//! │                                           │  │   (plan construction)     │  │   │
//! │                                           │  └───────────────────────────┘  │   │
//! │                                           └─────────────────┬───────────────┘   │
//! │                                                             │                   │
//! │                                                             ▼                   │
//! │                                           ┌─────────────────────────────────┐   │
//! │                                           │        LogicalPlan              │   │
//! │                                           │  (Scan, Filter, Project, ...)   │   │
//! │                                           └─────────────────────────────────┘   │
//! │                                                                                 │
//! └─────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Responsibilities
//!
//! 1. **SQL Parsing**: Uses `sqlparser` with `GenericDialect` to parse SQL strings into AST
//! 2. **Statement Dispatch**: Routes parsed statements to appropriate plan builders
//! 3. **Error Handling**: Validates single-statement input and propagates parsing errors
//! 4. **Plan Explanation**: Provides `explain()` for query plan visualization
//!
//! ## Supported Statements
//!
//! | Category       | Statement Types                                              |
//! |----------------|--------------------------------------------------------------|
//! | **DML**        | `SELECT`, `INSERT`, `UPDATE`, `DELETE`                       |
//! | **DDL**        | `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `ALTER TABLE` |
//! |                | `CREATE SCHEMA`, `CREATE DATABASE`, `ALTER VIEW`             |
//! | **Transaction**| `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`        |
//! | **Utility**    | `EXPLAIN`, `SHOW TABLES`, `SHOW DATABASES`, `SHOW COLUMNS`   |
//! |                | `USE`                                                        |
//!
//! ## Statement Routing
//!
//! ```text
//! Statement Type           LogicalPlanBuilder Method
//! ──────────────────────────────────────────────────────
//! Query (SELECT)      ──▶  build_query_plan()
//! Insert              ──▶  build_insert_plan()
//! CreateTable         ──▶  build_create_table_plan()
//! CreateIndex         ──▶  build_create_index_plan()
//! Update              ──▶  build_update_plan()
//! Delete              ──▶  build_delete_plan()
//! Explain             ──▶  build_explain_plan()
//! StartTransaction    ──▶  build_start_transaction_plan()
//! Commit              ──▶  build_commit_plan()
//! Rollback            ──▶  build_rollback_plan()
//! CreateSchema        ──▶  build_create_schema_plan()
//! CreateDatabase      ──▶  build_create_database_plan()
//! AlterTable          ──▶  build_alter_table_plan()
//! CreateView          ──▶  build_create_view_plan()
//! ShowTables          ──▶  build_show_tables_plan()
//! ...
//! ```
//!
//! ## Example Usage
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use parking_lot::RwLock;
//!
//! // Create planner with catalog
//! let catalog = Arc::new(RwLock::new(Catalog::new()));
//! let mut planner = QueryPlanner::new(catalog);
//!
//! // Create logical plan from SQL
//! let plan = planner.create_logical_plan("SELECT * FROM users WHERE id = 1")?;
//!
//! // Get query explanation
//! let explanation = planner.explain("SELECT * FROM users")?;
//! println!("{}", explanation);
//! // Output:
//! // Query Plan:
//! // Project [*]
//! //   TableScan: users
//! ```
//!
//! ## Error Handling
//!
//! - **Parse errors**: Propagated from `sqlparser` with error message
//! - **Multiple statements**: Rejected with "Only single SQL statement is supported"
//! - **Unsupported statements**: Returns "Unsupported statement type: ..."
//!
//! ## Thread Safety
//!
//! The `QueryPlanner` holds an `Arc<RwLock<Catalog>>` for thread-safe catalog access
//! during plan construction. The planner itself requires `&mut self` for planning
//! operations due to internal state in the `LogicalPlanBuilder`.

use super::logical_plan::LogicalPlan;
use crate::catalog::Catalog;
use crate::sql::planner::plan_builder::LogicalPlanBuilder;
use parking_lot::RwLock;
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::sync::Arc;

/// Entry point for SQL query planning and logical plan construction.
///
/// `QueryPlanner` is the primary interface for transforming SQL text into executable
/// logical plans. It coordinates the parsing phase (via `sqlparser`) and the planning
/// phase (via `LogicalPlanBuilder`), providing a clean separation between SQL syntax
/// and query semantics.
///
/// # Architecture
///
/// The planner operates in two distinct phases:
///
/// 1. **Parsing Phase**: SQL text is parsed into an Abstract Syntax Tree (AST) using
///    the `sqlparser` crate with `GenericDialect` for broad SQL compatibility.
///
/// 2. **Planning Phase**: The AST is transformed into a `LogicalPlan` tree by the
///    internal `LogicalPlanBuilder`, which resolves table/column references against
///    the catalog and constructs the appropriate plan nodes.
///
/// ```text
/// QueryPlanner
/// ┌─────────────────────────────────────────────────────────────┐
/// │                                                             │
/// │   ┌─────────────────────────────────────────────────────┐   │
/// │   │              LogicalPlanBuilder                     │   │
/// │   │  ┌─────────────────────────────────────────────┐    │   │
/// │   │  │           Arc<RwLock<Catalog>>              │    │   │
/// │   │  │  (table schemas, indexes, views, etc.)      │    │   │
/// │   │  └─────────────────────────────────────────────┘    │   │
/// │   └─────────────────────────────────────────────────────┘   │
/// │                                                             │
/// └─────────────────────────────────────────────────────────────┘
/// ```
///
/// # Thread Safety
///
/// While the `QueryPlanner` holds a thread-safe reference to the catalog
/// (`Arc<RwLock<Catalog>>`), the planner itself requires `&mut self` for planning
/// operations due to internal state management in the `LogicalPlanBuilder`.
/// For concurrent query planning, create separate `QueryPlanner` instances per thread.
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
/// use parking_lot::RwLock;
/// use ferrite::catalog::Catalog;
/// use ferrite::sql::planner::query_planner::QueryPlanner;
///
/// // Create a shared catalog
/// let catalog = Arc::new(RwLock::new(Catalog::new()));
///
/// // Create the query planner
/// let mut planner = QueryPlanner::new(catalog);
///
/// // Plan a SELECT query
/// let plan = planner.create_logical_plan("SELECT id, name FROM users WHERE active = true")?;
///
/// // Plan a DDL statement
/// let ddl_plan = planner.create_logical_plan("CREATE TABLE orders (id INT PRIMARY KEY)")?;
/// # Ok::<(), String>(())
/// ```
///
/// # Supported Statement Types
///
/// The planner supports the following SQL statement categories:
///
/// | Category        | Statements                                                   |
/// |-----------------|--------------------------------------------------------------|
/// | **Queries**     | `SELECT` (with joins, aggregates, subqueries, CTEs)          |
/// | **DML**         | `INSERT`, `UPDATE`, `DELETE`                                 |
/// | **DDL**         | `CREATE TABLE`, `CREATE INDEX`, `CREATE VIEW`, `ALTER TABLE` |
/// |                 | `CREATE SCHEMA`, `CREATE DATABASE`, `ALTER VIEW`             |
/// | **Transaction** | `BEGIN`, `COMMIT`, `ROLLBACK`, `SAVEPOINT`, `RELEASE`        |
/// | **Utility**     | `EXPLAIN`, `SHOW TABLES`, `SHOW DATABASES`, `SHOW COLUMNS`   |
/// |                 | `USE`                                                        |
pub struct QueryPlanner {
    /// The internal plan builder that performs the actual AST-to-LogicalPlan conversion.
    /// Holds a reference to the catalog for schema resolution.
    plan_builder: LogicalPlanBuilder,
}

impl QueryPlanner {
    /// Creates a new `QueryPlanner` with access to the given catalog.
    ///
    /// The catalog provides schema information (tables, columns, indexes, views)
    /// needed during the planning phase to validate and resolve references.
    ///
    /// # Arguments
    ///
    /// * `catalog` - A thread-safe reference to the database catalog. The planner
    ///   will acquire read locks on the catalog when resolving table and column
    ///   references during plan construction.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use parking_lot::RwLock;
    /// use ferrite::catalog::Catalog;
    /// use ferrite::sql::planner::query_planner::QueryPlanner;
    ///
    /// let catalog = Arc::new(RwLock::new(Catalog::new()));
    /// let planner = QueryPlanner::new(catalog);
    /// ```
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self {
            plan_builder: LogicalPlanBuilder::new(Arc::clone(&catalog)),
        }
    }

    /// Creates a logical plan from a SQL string.
    ///
    /// This is the primary entry point for query planning. The method:
    ///
    /// 1. **Parses** the SQL string into an AST using `sqlparser` with `GenericDialect`
    /// 2. **Validates** that exactly one statement was provided
    /// 3. **Delegates** to `create_logical_plan_from_statement` for plan construction
    ///
    /// # Arguments
    ///
    /// * `sql` - A SQL statement string. Must contain exactly one complete SQL statement.
    ///
    /// # Returns
    ///
    /// * `Ok(Box<LogicalPlan>)` - The constructed logical plan tree
    /// * `Err(String)` - An error message if parsing or planning fails
    ///
    /// # Errors
    ///
    /// Returns an error in the following cases:
    ///
    /// - **Parse error**: The SQL string contains syntax errors
    /// - **Multiple statements**: The input contains more than one SQL statement
    /// - **Unsupported statement**: The statement type is not implemented
    /// - **Schema error**: Referenced tables or columns don't exist in the catalog
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::sync::Arc;
    /// # use parking_lot::RwLock;
    /// # use ferrite::catalog::Catalog;
    /// # use ferrite::sql::planner::query_planner::QueryPlanner;
    /// let catalog = Arc::new(RwLock::new(Catalog::new()));
    /// let mut planner = QueryPlanner::new(catalog);
    ///
    /// // Successful planning
    /// let plan = planner.create_logical_plan("SELECT * FROM users")?;
    ///
    /// // Parse error
    /// let err = planner.create_logical_plan("SELEC * FROM users");
    /// assert!(err.is_err());
    ///
    /// // Multiple statements error
    /// let err = planner.create_logical_plan("SELECT 1; SELECT 2");
    /// assert!(err.is_err());
    /// # Ok::<(), String>(())
    /// ```
    pub fn create_logical_plan(&mut self, sql: &str) -> Result<Box<LogicalPlan>, String> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

        if ast.len() != 1 {
            return Err("Only single SQL statement is supported".to_string());
        }

        self.create_logical_plan_from_statement(&ast[0])
    }

    /// Creates a logical plan from a pre-parsed AST statement.
    ///
    /// This method is useful when you already have a parsed `Statement` from `sqlparser`,
    /// avoiding redundant parsing. It routes the statement to the appropriate builder
    /// method based on the statement type.
    ///
    /// # Statement Routing
    ///
    /// Each statement type is dispatched to a specialized builder method:
    ///
    /// | Statement          | Builder Method                    |
    /// |--------------------|-----------------------------------|
    /// | `Query`            | `build_query_plan()`              |
    /// | `Insert`           | `build_insert_plan()`             |
    /// | `CreateTable`      | `build_create_table_plan()`       |
    /// | `CreateIndex`      | `build_create_index_plan()`       |
    /// | `Update`           | `build_update_plan()`             |
    /// | `Delete`           | `build_delete_plan()`             |
    /// | `Explain`          | `build_explain_plan()`            |
    /// | `StartTransaction` | `build_start_transaction_plan()`  |
    /// | `Commit`           | `build_commit_plan()`             |
    /// | `Rollback`         | `build_rollback_plan()`           |
    /// | `Savepoint`        | `build_savepoint_plan()`          |
    /// | `ReleaseSavepoint` | `build_release_savepoint_plan()`  |
    /// | `CreateSchema`     | `build_create_schema_plan()`      |
    /// | `CreateDatabase`   | `build_create_database_plan()`    |
    /// | `AlterTable`       | `build_alter_table_plan()`        |
    /// | `CreateView`       | `build_create_view_plan()`        |
    /// | `AlterView`        | `build_alter_view_plan()`         |
    /// | `ShowTables`       | `build_show_tables_plan()`        |
    /// | `ShowDatabases`    | `build_show_databases_plan()`     |
    /// | `ShowColumns`      | `build_show_columns_plan()`       |
    /// | `Use`              | `build_use_plan()`                |
    ///
    /// # Arguments
    ///
    /// * `stmt` - A reference to a parsed `sqlparser::ast::Statement`
    ///
    /// # Returns
    ///
    /// * `Ok(Box<LogicalPlan>)` - The constructed logical plan tree
    /// * `Err(String)` - An error if the statement type is unsupported or planning fails
    ///
    /// # Errors
    ///
    /// - **Unsupported statement**: Returns `"Unsupported statement type: ..."`
    /// - **Schema errors**: Table or column not found in catalog
    /// - **Semantic errors**: Invalid SQL semantics (e.g., type mismatches, constraint violations)
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use sqlparser::dialect::GenericDialect;
    /// use sqlparser::parser::Parser;
    /// # use std::sync::Arc;
    /// # use parking_lot::RwLock;
    /// # use ferrite::catalog::Catalog;
    /// # use ferrite::sql::planner::query_planner::QueryPlanner;
    ///
    /// let sql = "SELECT id, name FROM users";
    /// let dialect = GenericDialect {};
    /// let ast = Parser::parse_sql(&dialect, sql).unwrap();
    ///
    /// let catalog = Arc::new(RwLock::new(Catalog::new()));
    /// let mut planner = QueryPlanner::new(catalog);
    /// let plan = planner.create_logical_plan_from_statement(&ast[0])?;
    /// # Ok::<(), String>(())
    /// ```
    pub fn create_logical_plan_from_statement(
        &mut self,
        stmt: &Statement,
    ) -> Result<Box<LogicalPlan>, String> {
        match stmt {
            Statement::Query(query) => self.plan_builder.build_query_plan(query),
            Statement::Insert(stmt) => self.plan_builder.build_insert_plan(stmt),
            Statement::CreateTable(stmt) => self.plan_builder.build_create_table_plan(stmt),
            Statement::CreateIndex(stmt) => self.plan_builder.build_create_index_plan(stmt),
            Statement::Update(update) => self.plan_builder.build_update_plan(
                &update.table,
                &update.assignments,
                &update.from,
                &update.selection,
                &update.returning,
                &update.or,
                &update.limit,
            ),
            Statement::Delete(_) => self.plan_builder.build_delete_plan(stmt),
            Statement::Explain { .. } => self.plan_builder.build_explain_plan(stmt),
            Statement::StartTransaction {
                modes,
                begin,
                transaction,
                modifier,
                statements,
                exception,
                has_end_keyword,
            } => self.plan_builder.build_start_transaction_plan(
                modes,
                begin,
                transaction,
                modifier,
                statements,
                exception,
                has_end_keyword,
            ),
            Statement::Commit {
                chain,
                modifier,
                end,
            } => self.plan_builder.build_commit_plan(chain, end, modifier),
            Statement::Rollback { chain, savepoint } => {
                self.plan_builder.build_rollback_plan(chain, savepoint)
            },
            Statement::Savepoint { name } => self.plan_builder.build_savepoint_plan(name),
            Statement::ReleaseSavepoint { name } => {
                self.plan_builder.build_release_savepoint_plan(name)
            },
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
                or_replace: _or_replace,
                transient: _transient,
                clone: _clone,
                data_retention_time_in_days: _data_retention_time_in_days,
                max_data_extension_time_in_days: _max_data_extension_time_in_days,
                external_volume: _external_volume,
                catalog: _catalog,
                replace_invalid_characters: _replace_invalid_characters,
                default_ddl_collation: _default_ddl_collation,
                storage_serialization_policy: _storage_serialization_policy,
                comment: _comment,
                catalog_sync: _catalog_sync,
                catalog_sync_namespace_mode: _catalog_sync_namespace_mode,
                catalog_sync_namespace_flatten_delimiter: _catalog_sync_namespace_flatten_delimiter,
                with_tags: _with_tags,
                with_contacts: _with_contacts,
            } => self.plan_builder.build_create_database_plan(
                db_name,
                if_not_exists,
                location,
                managed_location,
            ),
            Statement::AlterTable(alter) => {
                if alter.table_type.is_some() {
                    return Err("ALTER TABLE is only supported for regular tables".to_string());
                }
                self.plan_builder.build_alter_table_plan(
                    &alter.name,
                    &alter.if_exists,
                    &alter.only,
                    &alter.operations,
                    &alter.location,
                    &alter.on_cluster,
                )
            },
            Statement::CreateView(view) => self.plan_builder.build_create_view_plan(
                &view.or_alter,
                &view.or_replace,
                &view.materialized,
                &view.name,
                &view.columns,
                &view.query,
                &view.options,
                &view.cluster_by,
                &view.comment,
                &view.with_no_schema_binding,
                &view.if_not_exists,
                &view.temporary,
                &view.to,
                &view.params,
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

    /// Returns a human-readable explanation of the query plan for a SQL statement.
    ///
    /// This method provides query plan visualization by:
    /// 1. Creating the logical plan via `create_logical_plan()`
    /// 2. Calling the plan's `explain()` method to generate a textual representation
    ///
    /// The output shows the hierarchical structure of plan nodes with indentation
    /// indicating parent-child relationships.
    ///
    /// # Arguments
    ///
    /// * `sql` - A SQL statement string to explain
    ///
    /// # Returns
    ///
    /// * `Ok(String)` - A formatted query plan explanation
    /// * `Err(String)` - An error if parsing or planning fails
    ///
    /// # Output Format
    ///
    /// The explanation uses a tree format with indentation:
    ///
    /// ```text
    /// Query Plan:
    /// Projection: [id, name, email]
    ///   Filter: (age > 21)
    ///     TableScan: users
    /// ```
    ///
    /// For joins:
    ///
    /// ```text
    /// Query Plan:
    /// Projection: [o.id, u.name]
    ///   NestedLoopJoin: INNER ON (o.user_id = u.id)
    ///     TableScan: orders (alias: o)
    ///     TableScan: users (alias: u)
    /// ```
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use std::sync::Arc;
    /// # use parking_lot::RwLock;
    /// # use ferrite::catalog::Catalog;
    /// # use ferrite::sql::planner::query_planner::QueryPlanner;
    /// let catalog = Arc::new(RwLock::new(Catalog::new()));
    /// let mut planner = QueryPlanner::new(catalog);
    ///
    /// let explanation = planner.explain("SELECT * FROM users WHERE id = 1")?;
    /// println!("{}", explanation);
    /// // Output:
    /// // Query Plan:
    /// // Projection: [*]
    /// //   Filter: (id = 1)
    /// //     TableScan: users
    /// # Ok::<(), String>(())
    /// ```
    ///
    /// # Note
    ///
    /// This method is distinct from the SQL `EXPLAIN` statement handling. For
    /// `EXPLAIN SELECT ...` queries, use `create_logical_plan()` which returns
    /// a `LogicalPlan::Explain` node that can be executed.
    pub fn explain(&mut self, sql: &str) -> Result<String, String> {
        let plan = self.create_logical_plan(sql)?;
        Ok(format!("Query Plan:\n{}\n", plan.explain(0)))
    }
}
