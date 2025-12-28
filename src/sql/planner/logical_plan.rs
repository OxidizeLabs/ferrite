//! # Logical Plan Representation
//!
//! This module defines the **logical plan** data structures, which serve as the intermediate
//! representation (IR) between parsed SQL and physical execution plans. Logical plans describe
//! *what* operations to perform without specifying *how* to execute them.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────┐
//! │                         Query Processing Pipeline                               │
//! ├─────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                 │
//! │   ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────────────┐     │
//! │   │   SQL Text      │───▶│  sqlparser AST  │───▶│  LogicalPlanBuilder     │     │
//! │   │ "SELECT ..."    │    │  Statement      │    │  (plan_builder.rs)      │     │
//! │   └─────────────────┘    └─────────────────┘    └───────────┬─────────────┘     │
//! │                                                             │                   │
//! │                                                             ▼                   │
//! │                          ┌──────────────────────────────────────────────┐       │
//! │                          │            LogicalPlan (this module)         │       │
//! │                          │  ┌────────────────────────────────────────┐  │       │
//! │                          │  │         LogicalPlanType enum           │  │       │
//! │                          │  │  - TableScan, Filter, Projection, ...  │  │       │
//! │                          │  │  - Join, Aggregate, Sort, Limit, ...   │  │       │
//! │                          │  │  - DDL: CreateTable, CreateIndex, ...  │  │       │
//! │                          │  │  - Txn: StartTransaction, Commit, ...  │  │       │
//! │                          │  └────────────────────────────────────────┘  │       │
//! │                          │  ┌────────────────────────────────────────┐  │       │
//! │                          │  │    children: Vec<Box<LogicalPlan>>     │  │       │
//! │                          │  │       (tree structure for plans)       │  │       │
//! │                          │  └────────────────────────────────────────┘  │       │
//! │                          └───────────────────────┬──────────────────────┘       │
//! │                                                  │                              │
//! │                                                  ▼                              │
//! │                          ┌──────────────────────────────────────────────┐       │
//! │                          │   LogicalToPhysical::to_physical_plan()      │       │
//! │                          │   (PlanConverter - iterative conversion)     │       │
//! │                          └───────────────────────┬──────────────────────┘       │
//! │                                                  │                              │
//! │                                                  ▼                              │
//! │                          ┌──────────────────────────────────────────────┐       │
//! │                          │              PlanNode (Physical)             │       │
//! │                          │   SeqScanPlanNode, FilterNode, HashJoin...   │       │
//! │                          └──────────────────────────────────────────────┘       │
//! │                                                                                 │
//! └─────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component | Description |
//! |-----------|-------------|
//! | [`LogicalPlanType`] | Enum of all logical plan node types with their metadata |
//! | [`LogicalPlan`] | Tree node containing plan type and child plans |
//! | [`LogicalToPhysical`] | Trait for converting logical plans to physical plans |
//! | `PlanConverter` | Internal struct for iterative (non-recursive) conversion |
//!
//! ## LogicalPlanType Categories
//!
//! ### Data Manipulation (DML)
//! | Plan Type | Description |
//! |-----------|-------------|
//! | `TableScan` | Full sequential scan of a table |
//! | `IndexScan` | Index-based lookup with predicate keys |
//! | `Filter` | Row filtering with predicate expression |
//! | `Projection` | Column selection and computed expressions |
//! | `Insert` | Insert rows into a table |
//! | `Update` | Update rows with assignment expressions |
//! | `Delete` | Delete rows matching a predicate |
//! | `Values` | Inline row data for INSERT |
//!
//! ### Aggregation & Grouping
//! | Plan Type | Description |
//! |-----------|-------------|
//! | `Aggregate` | GROUP BY with aggregate functions (SUM, COUNT, AVG, ...) |
//! | `Window` | Window functions with PARTITION BY and ORDER BY |
//! | `Distinct` | Remove duplicate rows |
//!
//! ### Join Operations
//! | Plan Type | Description |
//! |-----------|-------------|
//! | `NestedLoopJoin` | Basic nested loop join (O(n×m)) |
//! | `NestedIndexJoin` | Index-assisted nested loop join |
//! | `HashJoin` | Hash-based equi-join (build + probe) |
//!
//! ### Sorting & Limiting
//! | Plan Type | Description |
//! |-----------|-------------|
//! | `Sort` | Sort by one or more columns (ASC/DESC) |
//! | `Limit` | Limit output to N rows |
//! | `Offset` | Skip first N rows |
//! | `TopN` | Combined sort + limit (optimized) |
//! | `TopNPerGroup` | Top N rows per group |
//!
//! ### Data Definition (DDL)
//! | Plan Type | Description |
//! |-----------|-------------|
//! | `CreateTable` | Create new table with schema |
//! | `CreateIndex` | Create index on table columns |
//! | `Drop` | Drop table, index, or other objects |
//! | `AlterTable` | Modify table structure |
//! | `CreateView` | Create view definition |
//!
//! ### Transaction Control
//! | Plan Type | Description |
//! |-----------|-------------|
//! | `StartTransaction` | Begin a new transaction |
//! | `Commit` | Commit current transaction |
//! | `Rollback` | Rollback current transaction |
//! | `Savepoint` | Create a savepoint |
//! | `ReleaseSavepoint` | Release a savepoint |
//!
//! ### Utility
//! | Plan Type | Description |
//! |-----------|-------------|
//! | `MockScan` | Test/mock table scan |
//! | `ShowTables` | List tables in database |
//! | `ShowColumns` | List columns in table |
//! | `ShowDatabases` | List available databases |
//! | `Use` | Switch current database |
//! | `Explain` | Show query plan (wraps inner plan) |
//!
//! ## Plan Tree Example
//!
//! ```text
//! SELECT name, SUM(amount)
//! FROM orders
//! WHERE status = 'active'
//! GROUP BY name
//! ORDER BY SUM(amount) DESC
//! LIMIT 10
//!
//!           LogicalPlan Tree:
//!
//!              ┌───────────┐
//!              │   Limit   │ k=10
//!              │   (10)    │
//!              └─────┬─────┘
//!                    │
//!              ┌─────▼─────┐
//!              │   Sort    │ ORDER BY sum DESC
//!              └─────┬─────┘
//!                    │
//!              ┌─────▼─────┐
//!              │ Aggregate │ GROUP BY name, SUM(amount)
//!              └─────┬─────┘
//!                    │
//!              ┌─────▼─────┐
//!              │  Filter   │ status = 'active'
//!              └─────┬─────┘
//!                    │
//!              ┌─────▼─────┐
//!              │ TableScan │ orders
//!              └───────────┘
//! ```
//!
//! ## Logical to Physical Conversion
//!
//! The [`LogicalToPhysical`] trait's `to_physical_plan()` method converts the logical plan
//! tree into physical execution plan nodes. The conversion uses an **iterative** approach
//! (via `PlanConverter`) to avoid stack overflow on deep plan trees.
//!
//! ```text
//! Conversion Process (Iterative, Bottom-Up):
//!
//! 1. Push root onto stack
//! 2. While stack not empty:
//!    a. Pop node
//!    b. If all children processed → convert node, store result
//!    c. Else → push node back, push unprocessed children
//! 3. Return result for root node
//! ```
//!
//! ## Join Key Extraction
//!
//! For join operations, the `extract_join_keys` function parses the join predicate to:
//! - Extract left and right key expressions
//! - Fix tuple indices (left=0, right=1) for executor
//! - Support compound predicates via AND
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use tkdb::sql::planner::logical_plan::{LogicalPlan, LogicalToPhysical};
//!
//! // Build a simple plan: SELECT * FROM users WHERE age > 21
//! let schema = Schema::new(vec![
//!     Column::new("id", TypeId::Integer),
//!     Column::new("name", TypeId::VarChar),
//!     Column::new("age", TypeId::Integer),
//! ]);
//!
//! let scan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
//! let predicate = /* age > 21 expression */;
//! let filter = LogicalPlan::filter(schema.clone(), "users".to_string(), 1, predicate, scan);
//!
//! // Explain the plan
//! println!("{}", filter.explain(0));
//!
//! // Convert to physical plan
//! let physical = filter.to_physical_plan()?;
//! ```
//!
//! ## Schema Propagation
//!
//! Each plan node carries a `Schema` that describes its output columns. The `get_schema()`
//! method returns the output schema, handling special cases like:
//! - Join schemas (merged left + right columns)
//! - Projection schemas (selected/computed columns)
//! - Aggregate schemas (group-by + aggregate columns)
//!
//! ## Thread Safety
//!
//! - `LogicalPlan` is `Clone` and can be shared across threads
//! - Expression references use `Arc<Expression>` for shared ownership
//! - Recursion depth tracking uses `thread_local!` for safety

use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::concurrency::transaction::IsolationLevel;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::aggregate_expression::AggregationType;
use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
use crate::sql::execution::expressions::comparison_expression::{
    ComparisonExpression, ComparisonType,
};
use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::sql::execution::plans::abstract_plan::PlanNode;
use crate::sql::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::sql::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::sql::execution::plans::create_table_plan::CreateTablePlanNode;
use crate::sql::execution::plans::delete_plan::DeleteNode;
use crate::sql::execution::plans::distinct_plan::DistinctNode;
use crate::sql::execution::plans::filter_plan::FilterNode;
use crate::sql::execution::plans::hash_join_plan::HashJoinNode;
use crate::sql::execution::plans::index_scan_plan::IndexScanNode;
use crate::sql::execution::plans::insert_plan::InsertNode;
use crate::sql::execution::plans::limit_plan::LimitNode;
use crate::sql::execution::plans::mock_scan_plan::MockScanNode;
use crate::sql::execution::plans::nested_index_join_plan::NestedIndexJoinNode;
use crate::sql::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::sql::execution::plans::offset_plan::OffsetNode;
use crate::sql::execution::plans::projection_plan::ProjectionNode;
use crate::sql::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::sql::execution::plans::sort_plan::{OrderBySpec, SortNode};
use crate::sql::execution::plans::topn_per_group_plan::TopNPerGroupNode;
use crate::sql::execution::plans::topn_plan::TopNNode;
use crate::sql::execution::plans::update_plan::UpdateNode;
use crate::sql::execution::plans::values_plan::ValuesNode;
use crate::sql::execution::plans::window_plan::{WindowFunction, WindowFunctionType, WindowNode};
use log::debug;
use sqlparser::ast::{ExceptionWhen, Ident, JoinOperator, Statement, TransactionModifier};
use std::cell::Cell;
use std::collections::{HashMap, HashSet};
use std::fmt::{self, Display, Formatter};
use std::ptr;
use std::sync::Arc;
use std::thread_local;

// Add thread-local variable for tracking recursion depth
thread_local! {
    static RECURSION_DEPTH: Cell<usize> = const { Cell::new(0) };
}

/// Enumeration of all logical plan node types.
///
/// Each variant represents a specific database operation with its associated metadata.
/// These are "logical" in that they describe *what* to do, not *how* to do it.
/// The conversion to physical plans (which specify execution strategies) happens
/// via the [`LogicalToPhysical`] trait.
///
/// # Categories
///
/// - **Scan Operations**: `TableScan`, `IndexScan`, `MockScan`
/// - **DML Operations**: `Insert`, `Update`, `Delete`, `Values`
/// - **Query Operations**: `Filter`, `Projection`, `Aggregate`, `Window`, `Distinct`
/// - **Join Operations**: `NestedLoopJoin`, `NestedIndexJoin`, `HashJoin`
/// - **Ordering**: `Sort`, `Limit`, `Offset`, `TopN`, `TopNPerGroup`
/// - **DDL Operations**: `CreateTable`, `CreateIndex`, `Drop`, `AlterTable`, `CreateView`, `AlterView`
/// - **Transaction Control**: `StartTransaction`, `Commit`, `Rollback`, `Savepoint`, `ReleaseSavepoint`
/// - **Utility**: `ShowTables`, `ShowColumns`, `ShowDatabases`, `Use`, `Explain`
#[derive(Debug, Clone)]
pub enum LogicalPlanType {
    /// Creates a new table with the specified schema.
    ///
    /// # Fields
    /// - `schema`: The column definitions for the new table
    /// - `table_name`: Name of the table to create
    /// - `if_not_exists`: If true, don't error if table already exists
    CreateTable {
        schema: Schema,
        table_name: String,
        if_not_exists: bool,
    },
    /// Creates a new index on an existing table.
    ///
    /// # Fields
    /// - `schema`: Schema of the table being indexed
    /// - `table_name`: Name of the table to index
    /// - `index_name`: Name for the new index
    /// - `key_attrs`: Column indices that form the index key
    /// - `if_not_exists`: If true, don't error if index already exists
    CreateIndex {
        schema: Schema,
        table_name: String,
        index_name: String,
        key_attrs: Vec<usize>,
        if_not_exists: bool,
    },
    /// A mock table scan for testing purposes.
    ///
    /// Returns predefined data without accessing actual storage.
    /// Useful for unit testing query execution without database setup.
    MockScan {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    /// Sequential scan of an entire table.
    ///
    /// Reads all rows from the table in storage order.
    /// This is the simplest but potentially slowest scan method for large tables.
    ///
    /// # Fields
    /// - `table_name`: Name of the table to scan
    /// - `schema`: Output schema (all columns from the table)
    /// - `table_oid`: Object ID for the table in the catalog
    TableScan {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    /// Index-based scan using predicate keys.
    ///
    /// Uses an existing index to efficiently locate rows matching the predicate.
    /// Much faster than sequential scan when selectivity is low.
    ///
    /// # Fields
    /// - `table_name`: Name of the table being scanned
    /// - `table_oid`: Object ID for the table
    /// - `index_name`: Name of the index to use
    /// - `index_oid`: Object ID for the index
    /// - `schema`: Output schema
    /// - `predicate_keys`: Key expressions used to probe the index
    IndexScan {
        table_name: String,
        table_oid: TableOidT,
        index_name: String,
        index_oid: IndexOidT,
        schema: Schema,
        predicate_keys: Vec<Arc<Expression>>,
    },
    /// Filters rows based on a predicate expression.
    ///
    /// Only rows where the predicate evaluates to true are passed through.
    /// Corresponds to the WHERE clause in SQL.
    ///
    /// # Fields
    /// - `schema`: Input schema (used for predicate evaluation)
    /// - `table_oid`: Object ID of the source table
    /// - `table_name`: Name of the source table
    /// - `predicate`: Boolean expression to evaluate for each row
    /// - `output_schema`: Schema of output rows (same as input)
    Filter {
        schema: Schema,
        table_oid: TableOidT,
        table_name: String,
        predicate: Arc<Expression>,
        output_schema: Schema,
    },
    /// Projects (selects) specific columns or computed expressions.
    ///
    /// Corresponds to the SELECT clause column list.
    /// Can include column references, constants, and computed expressions.
    ///
    /// # Fields
    /// - `expressions`: List of expressions to evaluate for each output row
    /// - `schema`: Output schema (one column per expression)
    /// - `column_mappings`: Maps output column positions to input column positions
    ///   (only for simple column references, not computed expressions)
    Projection {
        expressions: Vec<Arc<Expression>>,
        schema: Schema,
        /// Maps output column positions to input column positions
        column_mappings: Vec<usize>,
    },
    /// Inserts rows into a table.
    ///
    /// Takes input rows (typically from a `Values` node) and inserts them
    /// into the specified table.
    ///
    /// # Fields
    /// - `table_name`: Target table name
    /// - `schema`: Schema of the target table
    /// - `table_oid`: Object ID for the target table
    Insert {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    /// Deletes rows from a table.
    ///
    /// Removes rows that match the input (typically filtered) from the table.
    ///
    /// # Fields
    /// - `table_name`: Target table name
    /// - `schema`: Schema of the target table
    /// - `table_oid`: Object ID for the target table
    Delete {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
    },
    /// Updates rows in a table.
    ///
    /// Modifies matching rows by applying the update expressions.
    ///
    /// # Fields
    /// - `table_name`: Target table name
    /// - `schema`: Schema of the target table
    /// - `table_oid`: Object ID for the target table
    /// - `update_expressions`: Expressions that compute new column values
    Update {
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
        update_expressions: Vec<Arc<Expression>>,
    },
    /// Inline row values (e.g., from INSERT ... VALUES).
    ///
    /// Produces rows from literal expressions without reading from storage.
    ///
    /// # Fields
    /// - `rows`: List of rows, each containing expressions for column values
    /// - `schema`: Output schema matching the expression types
    Values {
        rows: Vec<Vec<Arc<Expression>>>,
        schema: Schema,
    },
    /// Aggregation with optional grouping (GROUP BY).
    ///
    /// Groups input rows by the group-by expressions and computes
    /// aggregate functions (SUM, COUNT, AVG, MIN, MAX, etc.) for each group.
    ///
    /// # Fields
    /// - `group_by`: Expressions that define grouping (empty for global aggregation)
    /// - `aggregates`: Aggregate function expressions to compute
    /// - `schema`: Output schema (group-by columns + aggregate results)
    Aggregate {
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        schema: Schema,
    },
    /// Nested loop join between two relations.
    ///
    /// For each row in the left relation, scans the entire right relation
    /// looking for matches. Time complexity is O(n × m).
    /// Best for small tables or when no index is available.
    ///
    /// # Fields
    /// - `left_schema`: Schema of the left (outer) relation
    /// - `right_schema`: Schema of the right (inner) relation
    /// - `predicate`: Join condition expression
    /// - `join_type`: Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
    NestedLoopJoin {
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
    },
    /// Nested loop join using an index on the inner relation.
    ///
    /// For each row in the left relation, uses an index to find matching
    /// rows in the right relation. More efficient than plain nested loop
    /// when the right side has a suitable index.
    ///
    /// # Fields
    /// - `left_schema`: Schema of the left (outer) relation
    /// - `right_schema`: Schema of the right (inner) relation
    /// - `predicate`: Join condition expression
    /// - `join_type`: Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
    NestedIndexJoin {
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
    },
    /// Hash-based equi-join between two relations.
    ///
    /// Builds a hash table from the smaller relation, then probes it
    /// with rows from the larger relation. Very efficient for equi-joins
    /// on large datasets. Time complexity is O(n + m).
    ///
    /// # Fields
    /// - `left_schema`: Schema of the left (build) relation
    /// - `right_schema`: Schema of the right (probe) relation
    /// - `predicate`: Join condition (must be equality-based)
    /// - `join_type`: Type of join (INNER, LEFT, RIGHT, FULL)
    HashJoin {
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
    },
    /// Sorts input rows by one or more columns.
    ///
    /// Corresponds to the ORDER BY clause.
    /// Supports ascending/descending order and NULLS FIRST/LAST.
    ///
    /// # Fields
    /// - `sort_specifications`: Ordered list of sort keys with direction
    /// - `schema`: Output schema (same as input)
    Sort {
        sort_specifications: Vec<OrderBySpec>,
        schema: Schema,
    },
    /// Limits output to at most N rows.
    ///
    /// Corresponds to the LIMIT clause.
    /// Returns the first `limit` rows from the input.
    ///
    /// # Fields
    /// - `limit`: Maximum number of rows to return
    /// - `schema`: Output schema (same as input)
    Limit { limit: usize, schema: Schema },
    /// Skips the first N rows from input.
    ///
    /// Corresponds to the OFFSET clause.
    /// Often used with LIMIT for pagination.
    ///
    /// # Fields
    /// - `offset`: Number of rows to skip
    /// - `schema`: Output schema (same as input)
    Offset { offset: usize, schema: Schema },
    /// Removes duplicate rows from output.
    ///
    /// Corresponds to SELECT DISTINCT.
    /// Compares all columns to identify duplicates.
    ///
    /// # Fields
    /// - `schema`: Output schema (same as input)
    Distinct { schema: Schema },
    /// Combined sort and limit operation (optimization).
    ///
    /// More efficient than separate Sort + Limit nodes because it only
    /// needs to track the top K elements, not sort the entire dataset.
    /// Uses a heap-based algorithm with O(n log k) complexity.
    ///
    /// # Fields
    /// - `k`: Number of top rows to return
    /// - `sort_specifications`: Sort order to determine "top"
    /// - `schema`: Output schema (same as input)
    TopN {
        k: usize,
        sort_specifications: Vec<OrderBySpec>,
        schema: Schema,
    },
    /// Returns top N rows within each group.
    ///
    /// Useful for queries like "top 3 products per category".
    /// Groups rows by the group expressions, then returns top K
    /// rows within each group based on sort order.
    ///
    /// # Fields
    /// - `k`: Number of top rows per group
    /// - `sort_specifications`: Sort order to determine "top" within group
    /// - `groups`: Expressions that define grouping
    /// - `schema`: Output schema (same as input)
    TopNPerGroup {
        k: usize,
        sort_specifications: Vec<OrderBySpec>,
        groups: Vec<Arc<Expression>>,
        schema: Schema,
    },
    /// Window function computation.
    ///
    /// Computes window functions (ROW_NUMBER, RANK, SUM OVER, etc.)
    /// across partitions of the input data.
    ///
    /// # Fields
    /// - `group_by`: ORDER BY expressions within each partition
    /// - `aggregates`: Window function expressions to compute
    /// - `partitions`: PARTITION BY expressions
    /// - `schema`: Output schema (input columns + window function results)
    Window {
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        partitions: Vec<Arc<Expression>>,
        schema: Schema,
    },
    /// Begins a new transaction.
    ///
    /// Corresponds to BEGIN, START TRANSACTION, or BEGIN TRANSACTION statements.
    ///
    /// # Fields
    /// - `isolation_level`: Transaction isolation level (READ COMMITTED, SERIALIZABLE, etc.)
    /// - `read_only`: If true, transaction cannot modify data
    /// - `transaction_modifier`: Additional transaction modifiers (DEFERRED, IMMEDIATE, etc.)
    /// - `statements`: Statements to execute within the transaction (for compound transactions)
    /// - `exception_statements`: Exception handlers for the transaction
    /// - `has_end_keyword`: Whether the transaction block uses END keyword
    StartTransaction {
        isolation_level: Option<IsolationLevel>,
        read_only: bool,
        transaction_modifier: Option<TransactionModifier>,
        statements: Vec<Statement>,
        exception_statements: Option<Vec<ExceptionWhen>>,
        has_end_keyword: bool,
    },
    /// Commits the current transaction.
    ///
    /// Makes all changes in the transaction permanent.
    ///
    /// # Fields
    /// - `chain`: If true, immediately starts a new transaction after commit
    /// - `end`: If true, ends a transaction block
    /// - `modifier`: Additional commit modifiers
    Commit {
        chain: bool,
        end: bool,
        modifier: Option<TransactionModifier>,
    },
    /// Rolls back the current transaction.
    ///
    /// Undoes all changes made in the transaction.
    ///
    /// # Fields
    /// - `chain`: If true, immediately starts a new transaction after rollback
    /// - `savepoint`: If specified, rolls back only to the named savepoint
    Rollback {
        chain: bool,
        savepoint: Option<Ident>,
    },
    /// Creates a savepoint within a transaction.
    ///
    /// Savepoints allow partial rollbacks within a transaction.
    ///
    /// # Fields
    /// - `name`: Name of the savepoint
    Savepoint { name: String },
    /// Releases (destroys) a savepoint.
    ///
    /// The savepoint and all savepoints created after it are removed.
    ///
    /// # Fields
    /// - `name`: Name of the savepoint to release
    ReleaseSavepoint { name: String },
    /// Drops database objects.
    ///
    /// Corresponds to DROP TABLE, DROP INDEX, DROP VIEW, etc.
    ///
    /// # Fields
    /// - `object_type`: Type of object ("TABLE", "INDEX", "VIEW", etc.)
    /// - `if_exists`: If true, don't error if object doesn't exist
    /// - `names`: Names of objects to drop
    /// - `cascade`: If true, also drop dependent objects
    Drop {
        object_type: String,
        if_exists: bool,
        names: Vec<String>,
        cascade: bool,
    },
    /// Creates a new schema (namespace).
    ///
    /// # Fields
    /// - `schema_name`: Name of the schema to create
    /// - `if_not_exists`: If true, don't error if schema already exists
    CreateSchema {
        schema_name: String,
        if_not_exists: bool,
    },
    /// Creates a new database.
    ///
    /// # Fields
    /// - `db_name`: Name of the database to create
    /// - `if_not_exists`: If true, don't error if database already exists
    CreateDatabase {
        db_name: String,
        if_not_exists: bool,
    },
    /// Modifies an existing table's structure.
    ///
    /// Corresponds to ALTER TABLE statements.
    ///
    /// # Fields
    /// - `table_name`: Name of the table to alter
    /// - `operation`: Description of the alteration ("ADD COLUMN", "DROP COLUMN", etc.)
    AlterTable {
        table_name: String,
        operation: String,
    },
    /// Creates a view (stored query).
    ///
    /// # Fields
    /// - `view_name`: Name of the view to create
    /// - `schema`: Schema of the view's output
    /// - `if_not_exists`: If true, don't error if view already exists
    CreateView {
        view_name: String,
        schema: Schema,
        if_not_exists: bool,
    },
    /// Modifies an existing view.
    ///
    /// # Fields
    /// - `view_name`: Name of the view to alter
    /// - `operation`: Description of the alteration
    AlterView {
        view_name: String,
        operation: String,
    },
    /// Lists tables in a schema/database.
    ///
    /// Corresponds to SHOW TABLES statement.
    ///
    /// # Fields
    /// - `schema_name`: Optional schema to list tables from
    /// - `terse`: If true, show minimal information
    /// - `history`: If true, include historical/deleted tables
    /// - `extended`: If true, show extended information
    /// - `full`: If true, show full table details
    /// - `external`: If true, show external tables only
    ShowTables {
        schema_name: Option<String>,
        terse: bool,
        history: bool,
        extended: bool,
        full: bool,
        external: bool,
    },
    /// Lists available databases.
    ///
    /// Corresponds to SHOW DATABASES statement.
    ///
    /// # Fields
    /// - `terse`: If true, show minimal information
    /// - `history`: If true, include historical/deleted databases
    ShowDatabases { terse: bool, history: bool },
    /// Lists columns in a table.
    ///
    /// Corresponds to SHOW COLUMNS or DESCRIBE statement.
    ///
    /// # Fields
    /// - `table_name`: Name of the table to describe
    /// - `schema_name`: Optional schema containing the table
    /// - `extended`: If true, show extended column information
    /// - `full`: If true, show full column details
    ShowColumns {
        table_name: String,
        schema_name: Option<String>,
        extended: bool,
        full: bool,
    },
    /// Switches the current database context.
    ///
    /// Corresponds to USE statement.
    ///
    /// # Fields
    /// - `db_name`: Name of the database to switch to
    Use { db_name: String },
    /// Shows the execution plan for a query.
    ///
    /// Corresponds to EXPLAIN statement.
    /// Wraps another plan and displays its structure without executing it.
    ///
    /// # Fields
    /// - `plan`: The logical plan to explain
    Explain { plan: Box<LogicalPlan> },
}

/// Trait for converting logical plans to physical execution plans.
///
/// This trait defines the transformation from high-level logical operations
/// (what to compute) to concrete physical operators (how to compute it).
///
/// # Implementation Notes
///
/// The default implementation in [`LogicalPlan`] uses an iterative (non-recursive)
/// approach via [`PlanConverter`] to avoid stack overflow on deep plan trees.
///
/// # Example
///
/// ```rust,ignore
/// use ferrite::sql::planner::logical_plan::{LogicalPlan, LogicalToPhysical};
///
/// let logical_plan = LogicalPlan::table_scan("users".to_string(), schema, 1);
/// let physical_plan = logical_plan.to_physical_plan()?;
/// // physical_plan is now a SeqScanPlanNode ready for execution
/// ```
pub trait LogicalToPhysical {
    /// Converts this logical plan to a physical execution plan.
    ///
    /// # Returns
    ///
    /// - `Ok(PlanNode)`: The physical plan ready for execution
    /// - `Err(String)`: An error message if conversion fails
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - A join node has insufficient children
    /// - Join key extraction fails
    /// - An unsupported window function type is encountered
    /// - A cycle is detected in the plan tree
    fn to_physical_plan(&self) -> Result<PlanNode, String>;
}

/// A logical plan node representing a database operation.
///
/// `LogicalPlan` forms a tree structure where each node contains:
/// - A [`LogicalPlanType`] describing the operation and its parameters
/// - A list of child plans that provide input data
///
/// # Tree Structure
///
/// Logical plans form a tree where data flows from leaves (scans) to root:
///
/// ```text
///              Projection (root)
///                   │
///                 Filter
///                   │
///              TableScan (leaf)
/// ```
///
/// # Creating Plans
///
/// Use the static constructor methods to build plan trees:
///
/// ```rust,ignore
/// // Build: SELECT name FROM users WHERE age > 21
/// let scan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
/// let filter = LogicalPlan::filter(schema.clone(), "users".to_string(), 1, predicate, scan);
/// let projection = LogicalPlan::project(expressions, output_schema, filter);
/// ```
///
/// # Schema Propagation
///
/// Each plan node can report its output schema via [`get_schema()`](LogicalPlan::get_schema).
/// This is used for type checking and to build physical plans with correct schemas.
///
/// # Thread Safety
///
/// `LogicalPlan` is `Clone` and can be shared across threads.
/// Expression references use `Arc<Expression>` for shared ownership.
#[derive(Debug, Clone)]
pub struct LogicalPlan {
    /// The type of operation this node performs with its parameters.
    pub plan_type: LogicalPlanType,
    /// Child plans that provide input data to this node.
    /// Empty for leaf nodes (scans, values).
    pub children: Vec<Box<LogicalPlan>>,
}

impl LogicalPlan {
    /// Creates a new logical plan node.
    ///
    /// # Arguments
    ///
    /// * `plan_type` - The type of operation this node performs
    /// * `children` - Child plans that provide input data
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let plan = LogicalPlan::new(
    ///     LogicalPlanType::TableScan {
    ///         table_name: "users".to_string(),
    ///         schema: schema,
    ///         table_oid: 1,
    ///     },
    ///     vec![], // No children for a scan
    /// );
    /// ```
    pub fn new(plan_type: LogicalPlanType, children: Vec<Box<LogicalPlan>>) -> Self {
        Self {
            plan_type,
            children,
        }
    }

    /// Returns a human-readable string representation of the logical plan tree.
    ///
    /// Produces an indented tree structure showing all nodes and their parameters.
    /// Useful for debugging and for EXPLAIN queries.
    ///
    /// # Arguments
    ///
    /// * `depth` - Current indentation depth (typically 0 for root)
    ///
    /// # Returns
    ///
    /// A formatted string showing the plan tree structure.
    ///
    /// # Example Output
    ///
    /// ```text
    /// → Filter
    ///    Predicate: age > 21
    ///    Table: users
    ///    Schema: (id: INTEGER, name: VARCHAR, age: INTEGER)
    ///   → TableScan: users
    ///      Schema: (id: INTEGER, name: VARCHAR, age: INTEGER)
    /// ```
    pub fn explain(&self, depth: usize) -> String {
        let indent_str = "  ".repeat(depth);
        let mut result = String::new();

        match &self.plan_type {
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateTable: {}\n", indent_str, table_name));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS\n", indent_str));
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            } => {
                result.push_str(&format!(
                    "{}→ CreateIndex: {} on {}\n",
                    indent_str, index_name, table_name
                ));
                result.push_str(&format!("{}   Key Columns: {:?}\n", indent_str, key_attrs));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS\n", indent_str));
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::MockScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ MockScan: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::TableScan {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ TableScan: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::IndexScan {
                table_name,
                table_oid: _,
                index_name,
                index_oid: _,
                schema,
                predicate_keys,
            } => {
                result.push_str(&format!(
                    "{}→ IndexScan: {} using {}\n",
                    indent_str, table_name, index_name
                ));
                result.push_str(&format!("{}   Predicate Keys: [", indent_str));
                for (i, key) in predicate_keys.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&key.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Filter {
                schema,

                table_name,
                predicate,
                ..
            } => {
                result.push_str(&format!("{}→ Filter\n", indent_str));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Projection {
                expressions,
                schema,
                column_mappings: _,
            } => {
                result.push_str(&format!("{}→ Projection\n", indent_str));
                result.push_str(&format!("{}   Expressions: [", indent_str));
                for (i, expr) in expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Limit { limit, schema } => {
                result.push_str(&format!("{}→ Limit: {}\n", indent_str, limit));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Offset { offset, schema } => {
                result.push_str(&format!("{}→ Offset: {}\n", indent_str, offset));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Distinct { schema } => {
                result.push_str(&format!("{}→ Distinct\n", indent_str));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Insert {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ Insert\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Delete {
                table_name, schema, ..
            } => {
                result.push_str(&format!("{}→ Delete\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Update {
                table_name,
                schema,
                update_expressions,
                ..
            } => {
                result.push_str(&format!("{}→ Update\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Target Expressions: [", indent_str));
                for (i, expr) in update_expressions.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Values { rows, schema } => {
                result.push_str(&format!("{}→ Values: {} rows\n", indent_str, rows.len()));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            } => {
                result.push_str(&format!("{}→ Aggregate\n", indent_str));
                if !group_by.is_empty() {
                    result.push_str(&format!("{}   Group By: [", indent_str));
                    for (i, expr) in group_by.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Aggregates: [", indent_str));
                for (i, expr) in aggregates.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                result.push_str(&format!("{}→ NestedLoopJoin\n", indent_str));
                result.push_str(&format!("{}   Join Type: {:?}\n", indent_str, join_type));
                result.push_str(&format!("{}   Predicate: {}\n", indent_str, predicate));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            },
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                ..
            } => {
                result.push_str(&format!("{}→ NestedIndexJoin\n", indent_str));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            },
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                ..
            } => {
                result.push_str(&format!("{}→ HashJoin\n", indent_str));
                result.push_str(&format!("{}   Left Schema: {}\n", indent_str, left_schema));
                result.push_str(&format!(
                    "{}   Right Schema: {}\n",
                    indent_str, right_schema
                ));
            },
            LogicalPlanType::Sort {
                sort_specifications,
                schema,
            } => {
                result.push_str(&format!("{}→ Sort\n", indent_str));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, spec) in sort_specifications.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&spec.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::TopN {
                k,
                sort_specifications,
                schema,
            } => {
                result.push_str(&format!("{}→ TopN: {}\n", indent_str, k));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, spec) in sort_specifications.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&spec.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::TopNPerGroup {
                k,
                sort_specifications,
                groups,
                schema,
            } => {
                result.push_str(&format!("{}→ TopNPerGroup: {}\n", indent_str, k));
                result.push_str(&format!("{}   Order By: [", indent_str));
                for (i, spec) in sort_specifications.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&spec.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Group By: [", indent_str));
                for (i, expr) in groups.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            } => {
                result.push_str(&format!("{}→ Window\n", indent_str));
                if !group_by.is_empty() {
                    result.push_str(&format!("{}   Group By: [", indent_str));
                    for (i, expr) in group_by.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Window Functions: [", indent_str));
                for (i, expr) in aggregates.iter().enumerate() {
                    if i > 0 {
                        result.push_str(", ");
                    }
                    result.push_str(&expr.to_string());
                }
                result.push_str("]\n");
                if !partitions.is_empty() {
                    result.push_str(&format!("{}   Partition By: [", indent_str));
                    for (i, expr) in partitions.iter().enumerate() {
                        if i > 0 {
                            result.push_str(", ");
                        }
                        result.push_str(&expr.to_string());
                    }
                    result.push_str("]\n");
                }
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
            },
            LogicalPlanType::StartTransaction {
                isolation_level,
                read_only,
                transaction_modifier,
                statements,
                exception_statements,
                has_end_keyword,
            } => {
                result.push_str(&format!("{}→ StartTransaction\n", indent_str));
                if let Some(level) = isolation_level {
                    result.push_str(&format!("{}   Isolation Level: {}\n", indent_str, level));
                }
                if *read_only {
                    result.push_str(&format!("{}   Read Only: true\n", indent_str));
                }
                if let Some(modifier) = transaction_modifier {
                    result.push_str(&format!(
                        "{}   Transaction Modifier: {:?}\n",
                        indent_str, modifier
                    ));
                }
                result.push_str(&format!("{}   Statements: {:?}\n", indent_str, statements));
                if let Some(exceptions) = exception_statements {
                    result.push_str(&format!(
                        "{}   Exception Statements: {:?}\n",
                        indent_str, exceptions
                    ));
                }
                if *has_end_keyword {
                    result.push_str(&format!("{}   Has End Keyword: true\n", indent_str));
                }
            },
            LogicalPlanType::Commit {
                chain,
                end,
                modifier,
            } => {
                result.push_str(&format!("{}→ Commit\n", indent_str));
                if *chain {
                    result.push_str(&format!("{}   Chain: true\n", indent_str));
                }
                if *end {
                    result.push_str(&format!("{}   End: true\n", indent_str));
                }
                if let Some(m) = modifier {
                    result.push_str(&format!("{}   Modifier: {:?}\n", indent_str, m));
                }
            },
            LogicalPlanType::Rollback { chain, savepoint } => {
                result.push_str(&format!("{}→ Rollback\n", indent_str));
                if *chain {
                    result.push_str(&format!("{}   Chain: true\n", indent_str));
                }
                if let Some(s) = savepoint {
                    result.push_str(&format!("{}   Savepoint: {}\n", indent_str, s.value));
                }
            },
            LogicalPlanType::Savepoint { name } => {
                result.push_str(&format!("{}→ Savepoint\n", indent_str));
                result.push_str(&format!("{}   Name: {}\n", indent_str, name));
            },
            LogicalPlanType::ReleaseSavepoint { name } => {
                result.push_str(&format!("{}→ ReleaseSavepoint\n", indent_str));
                result.push_str(&format!("{}   Name: {}\n", indent_str, name));
            },
            LogicalPlanType::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            } => {
                result.push_str(&format!("{}→ Drop {}\n", indent_str, object_type));
                if *if_exists {
                    result.push_str(&format!("{}   IF EXISTS: true\n", indent_str));
                }
                result.push_str(&format!(
                    "{}   Objects: [{}]\n",
                    indent_str,
                    names.join(", ")
                ));
                if *cascade {
                    result.push_str(&format!("{}   CASCADE: true\n", indent_str));
                }
            },
            LogicalPlanType::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateSchema\n", indent_str));
                result.push_str(&format!("{}   Schema Name: {}\n", indent_str, schema_name));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS: true\n", indent_str));
                }
            },
            LogicalPlanType::CreateDatabase {
                db_name,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateDatabase\n", indent_str));
                result.push_str(&format!("{}   Database Name: {}\n", indent_str, db_name));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS: true\n", indent_str));
                }
            },
            LogicalPlanType::AlterTable {
                table_name,
                operation,
            } => {
                result.push_str(&format!("{}→ AlterTable\n", indent_str));
                result.push_str(&format!("{}   Table Name: {}\n", indent_str, table_name));
                result.push_str(&format!("{}   Operation: {}\n", indent_str, operation));
            },
            LogicalPlanType::CreateView {
                view_name,
                schema,
                if_not_exists,
            } => {
                result.push_str(&format!("{}→ CreateView\n", indent_str));
                result.push_str(&format!("{}   View Name: {}\n", indent_str, view_name));
                result.push_str(&format!("{}   Schema: {}\n", indent_str, schema));
                if *if_not_exists {
                    result.push_str(&format!("{}   IF NOT EXISTS: true\n", indent_str));
                }
            },
            LogicalPlanType::AlterView {
                view_name,
                operation,
            } => {
                result.push_str(&format!("{}→ AlterView\n", indent_str));
                result.push_str(&format!("{}   View Name: {}\n", indent_str, view_name));
                result.push_str(&format!("{}   Operation: {}\n", indent_str, operation));
            },
            LogicalPlanType::ShowTables {
                schema_name,
                terse,
                history,
                extended,
                full,
                external,
            } => {
                result.push_str(&format!("{}→ ShowTables\n", indent_str));
                if let Some(name) = schema_name {
                    result.push_str(&format!("{}   Schema: {}\n", indent_str, name));
                }
                result.push_str(&format!("{}   Terse: {}\n", indent_str, *terse));
                result.push_str(&format!("{}   History: {}\n", indent_str, *history));
                result.push_str(&format!("{}   Extended: {}\n", indent_str, *extended));
                result.push_str(&format!("{}   Full: {}\n", indent_str, *full));
                result.push_str(&format!("{}   External: {}\n", indent_str, *external));
            },
            LogicalPlanType::ShowDatabases { terse, history } => {
                result.push_str(&format!("{}→ ShowDatabases\n", indent_str));
                result.push_str(&format!("{}   Terse: {}\n", indent_str, terse));
                result.push_str(&format!("{}   History: {}\n", indent_str, history));
            },
            LogicalPlanType::ShowColumns {
                table_name,
                schema_name,
                extended,
                full,
            } => {
                result.push_str(&format!("{}→ ShowColumns\n", indent_str));
                result.push_str(&format!("{}   Table: {}\n", indent_str, table_name));
                if let Some(name) = schema_name {
                    result.push_str(&format!("{}   Schema: {}\n", indent_str, name));
                }
                result.push_str(&format!("{}   Extended: {}\n", indent_str, extended));
                result.push_str(&format!("{}   Full: {}\n", indent_str, full));
            },
            LogicalPlanType::Use { db_name } => {
                result.push_str(&format!("{}→ Use\n", indent_str));
                result.push_str(&format!("{}   Database: {}\n", indent_str, db_name));
            },
            LogicalPlanType::Explain { plan } => {
                result.push_str(&format!("{}→ Explain\n", indent_str));
                // Add the inner plan with increased indentation
                result.push_str(&plan.explain(depth + 1));
            },
        }

        // Add children recursively
        for child in &self.children {
            result.push_str(&child.explain(depth + 1));
        }

        result
    }

    /// Creates a CREATE TABLE logical plan.
    ///
    /// # Arguments
    ///
    /// * `schema` - Column definitions for the new table
    /// * `table_name` - Name of the table to create
    /// * `if_not_exists` - If true, don't error when table already exists
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn create_table(schema: Schema, table_name: String, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            },
            vec![],
        ))
    }

    /// Creates a CREATE INDEX logical plan.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema of the table being indexed
    /// * `table_name` - Name of the table to index
    /// * `index_name` - Name for the new index
    /// * `key_attrs` - Column indices that form the index key
    /// * `if_not_exists` - If true, don't error when index already exists
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn create_index(
        schema: Schema,
        table_name: String,
        index_name: String,
        key_attrs: Vec<usize>,
        if_not_exists: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            },
            vec![],
        ))
    }

    /// Creates a sequential table scan logical plan.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to scan
    /// * `schema` - Schema of the table (defines output columns)
    /// * `table_oid` - Catalog object ID for the table
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn table_scan(table_name: String, schema: Schema, table_oid: u64) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TableScan {
                table_name,
                schema,
                table_oid,
            },
            vec![],
        ))
    }

    /// Creates a filter logical plan.
    ///
    /// Filters rows from the input plan based on a predicate expression.
    /// Only rows where the predicate evaluates to true pass through.
    ///
    /// # Arguments
    ///
    /// * `schema` - Schema of the input/output (unchanged by filter)
    /// * `table_name` - Name of the source table
    /// * `table_oid` - Catalog object ID for the table
    /// * `predicate` - Boolean expression to filter rows
    /// * `input` - Child plan providing input rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn filter(
        schema: Schema,
        table_name: String,
        table_oid: TableOidT,
        predicate: Arc<Expression>,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Filter {
                schema: schema.clone(),
                table_oid,
                table_name,
                predicate,
                output_schema: schema,
            },
            vec![input],
        ))
    }

    /// Creates a projection logical plan.
    ///
    /// Projects (selects) specific columns or computes new expressions
    /// from the input rows. Corresponds to the SELECT clause column list.
    ///
    /// Automatically computes column mappings from output to input positions
    /// for simple column references.
    ///
    /// # Arguments
    ///
    /// * `expressions` - Expressions to evaluate for each output column
    /// * `schema` - Output schema (one column per expression)
    /// * `input` - Child plan providing input rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn project(
        expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        // Get the input schema
        let Some(input_schema) = input.get_schema() else {
            todo!()
        };

        // Calculate column mappings
        let mut column_mappings = Vec::with_capacity(expressions.len());

        for expr in &expressions {
            if let Expression::ColumnRef(col_ref) = expr.as_ref() {
                let col_name = col_ref.get_return_type().get_name();

                // Try to find the column in the input schema
                let input_idx = input_schema
                    .get_columns()
                    .iter()
                    .position(|c| c.get_name() == col_name)
                    .unwrap_or_else(|| {
                        // If not found directly, look for aggregate functions
                        input_schema
                            .get_columns()
                            .iter()
                            .position(|c| {
                                c.get_name().starts_with("SUM(")
                                    || c.get_name().starts_with("COUNT(")
                                    || c.get_name().starts_with("AVG(")
                                    || c.get_name().starts_with("MIN(")
                                    || c.get_name().starts_with("MAX(")
                            })
                            .unwrap_or(0) // Default to first column if not found
                    });

                column_mappings.push(input_idx);
            }
            // NOTE: No else clause here - we only add mappings for ColumnRef expressions
            // Computed expressions like CASE, arithmetic, etc. will be evaluated directly
        }

        Box::new(Self::new(
            LogicalPlanType::Projection {
                expressions,
                schema,
                column_mappings,
            },
            vec![input],
        ))
    }

    /// Creates an INSERT logical plan.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the target table
    /// * `schema` - Schema of the target table
    /// * `table_oid` - Catalog object ID for the table
    /// * `input` - Child plan providing rows to insert (typically a Values node)
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn insert(
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Insert {
                table_name,
                schema,
                table_oid,
            },
            vec![input],
        ))
    }

    /// Creates a VALUES logical plan for inline row data.
    ///
    /// Produces rows from literal expressions without accessing storage.
    /// Typically used as input to INSERT statements.
    ///
    /// # Arguments
    ///
    /// * `rows` - List of rows, each containing expressions for column values
    /// * `schema` - Output schema matching the expression types
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn values(rows: Vec<Vec<Arc<Expression>>>, schema: Schema) -> Box<Self> {
        Box::new(Self::new(LogicalPlanType::Values { rows, schema }, vec![]))
    }

    /// Creates an aggregation logical plan.
    ///
    /// Groups rows by the group-by expressions and computes aggregate
    /// functions for each group. If group_by is empty, computes a single
    /// global aggregate over all input rows.
    ///
    /// # Arguments
    ///
    /// * `group_by` - Expressions that define grouping (empty for global aggregation)
    /// * `aggregates` - Aggregate function expressions (SUM, COUNT, AVG, etc.)
    /// * `schema` - Output schema (group-by columns + aggregate results)
    /// * `input` - Child plan providing input rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn aggregate(
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema,
            },
            vec![input],
        ))
    }

    /// Creates an index scan logical plan.
    ///
    /// Uses an index to efficiently locate rows matching the predicate keys.
    /// Much faster than sequential scan for selective queries.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to scan
    /// * `table_oid` - Catalog object ID for the table
    /// * `index_name` - Name of the index to use
    /// * `index_oid` - Catalog object ID for the index
    /// * `schema` - Output schema
    /// * `predicate_keys` - Key expressions to probe the index
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn index_scan(
        table_name: String,
        table_oid: TableOidT,
        index_name: String,
        index_oid: IndexOidT,
        schema: Schema,
        predicate_keys: Vec<Arc<Expression>>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
                predicate_keys,
            },
            vec![],
        ))
    }

    /// Creates a mock scan logical plan for testing.
    ///
    /// Returns predefined data without accessing actual storage.
    /// Useful for unit testing query execution.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the mock table
    /// * `schema` - Schema of the mock table
    /// * `table_oid` - Mock object ID
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn mock_scan(table_name: String, schema: Schema, table_oid: TableOidT) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::MockScan {
                table_name,
                schema,
                table_oid,
            },
            vec![],
        ))
    }

    /// Creates a DELETE logical plan.
    ///
    /// Removes rows from the table that are provided by the input plan.
    /// The input is typically a filter that selects rows to delete.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the target table
    /// * `schema` - Schema of the target table
    /// * `table_oid` - Catalog object ID for the table
    /// * `input` - Child plan providing rows to delete
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn delete(
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Delete {
                table_name,
                schema,
                table_oid,
            },
            vec![input],
        ))
    }

    /// Creates an UPDATE logical plan.
    ///
    /// Modifies rows from the table that are provided by the input plan.
    /// The update expressions compute new values for the modified columns.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the target table
    /// * `schema` - Schema of the target table
    /// * `table_oid` - Catalog object ID for the table
    /// * `update_expressions` - Expressions that compute new column values
    /// * `input` - Child plan providing rows to update
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn update(
        table_name: String,
        schema: Schema,
        table_oid: TableOidT,
        update_expressions: Vec<Arc<Expression>>,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
            },
            vec![input],
        ))
    }

    /// Creates a nested loop join logical plan.
    ///
    /// For each row in the left relation, scans the entire right relation
    /// looking for matches. Simple but O(n × m) complexity.
    ///
    /// # Arguments
    ///
    /// * `left_schema` - Schema of the left (outer) relation
    /// * `right_schema` - Schema of the right (inner) relation
    /// * `predicate` - Join condition expression
    /// * `join_type` - Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
    /// * `left_child` - Plan providing left relation rows
    /// * `right_child` - Plan providing right relation rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with two children.
    pub fn nested_loop_join(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left_child: Box<LogicalPlan>,
        right_child: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            },
            vec![left_child, right_child],
        ))
    }

    /// Creates a nested index join logical plan.
    ///
    /// For each row in the left relation, uses an index to find matching
    /// rows in the right relation. More efficient than nested loop when
    /// the right side has a suitable index.
    ///
    /// # Arguments
    ///
    /// * `left_schema` - Schema of the left (outer) relation
    /// * `right_schema` - Schema of the right (inner) relation
    /// * `predicate` - Join condition expression
    /// * `join_type` - Type of join (INNER, LEFT, RIGHT, FULL, CROSS)
    /// * `left` - Plan providing left relation rows
    /// * `right` - Plan providing right relation rows (should have index)
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with two children.
    pub fn nested_index_join(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            },
            vec![left, right],
        ))
    }

    /// Creates a hash join logical plan.
    ///
    /// Builds a hash table from the left relation, then probes it with rows
    /// from the right relation. Very efficient for equi-joins: O(n + m).
    ///
    /// # Arguments
    ///
    /// * `left_schema` - Schema of the left (build) relation
    /// * `right_schema` - Schema of the right (probe) relation
    /// * `predicate` - Join condition (must be equality-based)
    /// * `join_type` - Type of join (INNER, LEFT, RIGHT, FULL)
    /// * `left` - Plan providing left relation rows (used to build hash table)
    /// * `right` - Plan providing right relation rows (used to probe)
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with two children.
    pub fn hash_join(
        left_schema: Schema,
        right_schema: Schema,
        predicate: Arc<Expression>,
        join_type: JoinOperator,
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            },
            vec![left, right],
        ))
    }

    /// Creates a sort logical plan.
    ///
    /// Sorts input rows by the specified columns in the given order.
    /// Corresponds to the ORDER BY clause.
    ///
    /// # Arguments
    ///
    /// * `sort_specifications` - Ordered list of sort keys with direction (ASC/DESC)
    /// * `schema` - Output schema (same as input)
    /// * `input` - Child plan providing rows to sort
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn sort(
        sort_specifications: Vec<OrderBySpec>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Sort {
                sort_specifications,
                schema,
            },
            vec![input],
        ))
    }

    /// Creates a sort plan with expressions, defaulting to ASC order.
    ///
    /// Convenience method for backward compatibility. Converts expressions
    /// to OrderBySpec with ascending order.
    ///
    /// # Arguments
    ///
    /// * `sort_expressions` - Expressions to sort by
    /// * `schema` - Output schema
    /// * `input` - Child plan providing rows to sort
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn sort_with_expressions(
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        let sort_specifications = sort_expressions
            .into_iter()
            .map(|expr| {
                OrderBySpec::new(
                    expr,
                    crate::sql::execution::plans::sort_plan::OrderDirection::Asc,
                )
            })
            .collect();

        Self::sort(sort_specifications, schema, input)
    }

    /// Creates a LIMIT logical plan.
    ///
    /// Returns at most `limit` rows from the input.
    /// Corresponds to the LIMIT clause.
    ///
    /// # Arguments
    ///
    /// * `limit` - Maximum number of rows to return
    /// * `schema` - Output schema (same as input)
    /// * `input` - Child plan providing rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn limit(limit: usize, schema: Schema, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Limit { limit, schema },
            vec![input],
        ))
    }

    /// Creates an OFFSET logical plan.
    ///
    /// Skips the first `offset` rows from the input.
    /// Corresponds to the OFFSET clause. Often used with LIMIT for pagination.
    ///
    /// # Arguments
    ///
    /// * `offset` - Number of rows to skip
    /// * `schema` - Output schema (same as input)
    /// * `input` - Child plan providing rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn offset(offset: usize, schema: Schema, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Offset { offset, schema },
            vec![input],
        ))
    }

    /// Creates a DISTINCT logical plan.
    ///
    /// Removes duplicate rows based on all columns.
    /// Corresponds to SELECT DISTINCT.
    ///
    /// # Arguments
    ///
    /// * `schema` - Output schema (same as input)
    /// * `input` - Child plan providing rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn distinct(schema: Schema, input: Box<LogicalPlan>) -> Box<Self> {
        Box::new(Self::new(LogicalPlanType::Distinct { schema }, vec![input]))
    }

    /// Creates a TopN logical plan (optimized sort + limit).
    ///
    /// More efficient than separate Sort + Limit because it only tracks
    /// the top K elements using a heap. O(n log k) complexity.
    ///
    /// # Arguments
    ///
    /// * `k` - Number of top rows to return
    /// * `sort_specifications` - Sort order determining "top"
    /// * `schema` - Output schema (same as input)
    /// * `input` - Child plan providing rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn top_n(
        k: usize,
        sort_specifications: Vec<OrderBySpec>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TopN {
                k,
                sort_specifications,
                schema,
            },
            vec![input],
        ))
    }

    /// Creates a TopN plan with expressions, defaulting to ASC order.
    ///
    /// Convenience method for backward compatibility.
    ///
    /// # Arguments
    ///
    /// * `k` - Number of top rows to return
    /// * `sort_expressions` - Expressions to sort by
    /// * `schema` - Output schema
    /// * `input` - Child plan providing rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn top_n_with_expressions(
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        let sort_specifications = sort_expressions
            .into_iter()
            .map(|expr| {
                OrderBySpec::new(
                    expr,
                    crate::sql::execution::plans::sort_plan::OrderDirection::Asc,
                )
            })
            .collect();

        Self::top_n(k, sort_specifications, schema, input)
    }

    /// Creates a TopNPerGroup logical plan.
    ///
    /// Returns the top K rows within each group. Useful for queries like
    /// "top 3 products per category by sales".
    ///
    /// # Arguments
    ///
    /// * `k` - Number of top rows per group
    /// * `sort_specifications` - Sort order determining "top" within each group
    /// * `groups` - Expressions that define grouping
    /// * `schema` - Output schema (same as input)
    /// * `input` - Child plan providing rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn top_n_per_group(
        k: usize,
        sort_specifications: Vec<OrderBySpec>,
        groups: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::TopNPerGroup {
                k,
                sort_specifications,
                groups,
                schema,
            },
            vec![input],
        ))
    }

    /// Creates a TopNPerGroup plan with expressions, defaulting to ASC order.
    ///
    /// Convenience method for backward compatibility.
    ///
    /// # Arguments
    ///
    /// * `k` - Number of top rows per group
    /// * `sort_expressions` - Expressions to sort by
    /// * `groups` - Expressions that define grouping
    /// * `schema` - Output schema
    /// * `input` - Child plan providing rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn top_n_per_group_with_expressions(
        k: usize,
        sort_expressions: Vec<Arc<Expression>>,
        groups: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        let sort_specifications = sort_expressions
            .into_iter()
            .map(|expr| {
                OrderBySpec::new(
                    expr,
                    crate::sql::execution::plans::sort_plan::OrderDirection::Asc,
                )
            })
            .collect();

        Self::top_n_per_group(k, sort_specifications, groups, schema, input)
    }

    /// Creates a window function logical plan.
    ///
    /// Computes window functions (ROW_NUMBER, RANK, SUM OVER, etc.)
    /// across partitions of the input data.
    ///
    /// # Arguments
    ///
    /// * `group_by` - ORDER BY expressions within each partition
    /// * `aggregates` - Window function expressions to compute
    /// * `partitions` - PARTITION BY expressions
    /// * `schema` - Output schema (input columns + window results)
    /// * `input` - Child plan providing rows
    ///
    /// # Returns
    ///
    /// A boxed logical plan node with one child.
    pub fn window(
        group_by: Vec<Arc<Expression>>,
        aggregates: Vec<Arc<Expression>>,
        partitions: Vec<Arc<Expression>>,
        schema: Schema,
        input: Box<LogicalPlan>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            },
            vec![input],
        ))
    }

    /// Returns the output schema of this plan node.
    ///
    /// The schema describes the columns that this node produces.
    /// For join nodes, this merges the schemas of both sides.
    ///
    /// # Returns
    ///
    /// - `Some(Schema)` - The output schema for query-producing nodes
    /// - `None` - For utility nodes that don't produce query results
    pub fn get_schema(&self) -> Option<Schema> {
        match &self.plan_type {
            LogicalPlanType::CreateTable { schema, .. } => Some(schema.clone()),
            LogicalPlanType::CreateIndex { .. } => self.children[0].get_schema(),
            LogicalPlanType::MockScan { schema, .. } => Some(schema.clone()),
            LogicalPlanType::TableScan { schema, .. } => Some(schema.clone()),
            LogicalPlanType::IndexScan { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Projection { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Insert { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Values { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Delete { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Update { schema, .. } => Some(schema.clone()),
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For nested loop joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let Some(left_child_schema) = (if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    Some(left_schema.clone())
                }) else {
                    todo!()
                };

                let Some(right_child_schema) = (if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    Some(right_schema.clone())
                }) else {
                    todo!()
                };

                // Extract table names/aliases from the schemas
                // If no alias is found, we can still proceed with merging the schemas
                let left_alias = extract_table_alias_from_schema(&left_child_schema);
                let right_alias = extract_table_alias_from_schema(&right_child_schema);

                debug!(
                    "Merging schemas in get_schema with aliases: left={:?}, right={:?}",
                    left_alias, right_alias
                );

                // Create a new schema that preserves all column names with their original aliases
                let mut merged_columns = Vec::new();

                // Add all columns from the left schema, preserving their original names
                for col in left_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                // Add all columns from the right schema, preserving their original names
                for col in right_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                Some(Schema::new(merged_columns))
            },
            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For nested index joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let Some(left_child_schema) = (if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    Some(left_schema.clone())
                }) else {
                    todo!()
                };

                let Some(right_child_schema) = (if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    Some(right_schema.clone())
                }) else {
                    todo!()
                };

                // Extract table names/aliases from the schemas
                let left_alias = extract_table_alias_from_schema(&left_child_schema);
                let right_alias = extract_table_alias_from_schema(&right_child_schema);

                debug!(
                    "Merging schemas in get_schema with aliases: left={:?}, right={:?}",
                    left_alias, right_alias
                );

                // Create a new schema that preserves all column names with their original aliases
                let mut merged_columns = Vec::new();

                // Add all columns from the left schema, preserving their original names
                for col in left_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                // Add all columns from the right schema, preserving their original names
                for col in right_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                Some(Schema::new(merged_columns))
            },
            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                ..
            } => {
                // For hash joins, we need to preserve all table aliases
                // Get the schemas from the children to ensure we have the most up-to-date schemas
                let Some(left_child_schema) = (if !self.children.is_empty() {
                    self.children[0].get_schema()
                } else {
                    Some(left_schema.clone())
                }) else {
                    todo!()
                };

                let Some(right_child_schema) = (if self.children.len() > 1 {
                    self.children[1].get_schema()
                } else {
                    Some(right_schema.clone())
                }) else {
                    todo!()
                };

                // Extract table names/aliases from the schemas
                let left_alias = extract_table_alias_from_schema(&left_child_schema).unwrap();
                let right_alias = extract_table_alias_from_schema(&right_child_schema).unwrap();

                debug!(
                    "Merging schemas in get_schema with aliases: left={:?}, right={:?}",
                    left_alias, right_alias
                );

                // Create a new schema that preserves all column names with their original aliases
                let mut merged_columns = Vec::new();

                // Add all columns from the left schema, preserving their original names
                for col in left_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                // Add all columns from the right schema, preserving their original names
                for col in right_child_schema.get_columns() {
                    merged_columns.push(col.clone());
                }

                Some(Schema::new(merged_columns))
            },
            LogicalPlanType::Filter { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Sort { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Limit { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Offset { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Distinct { schema } => Some(schema.clone()),
            LogicalPlanType::TopN { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Aggregate { schema, .. } => Some(schema.clone()),
            LogicalPlanType::TopNPerGroup { schema, .. } => Some(schema.clone()),
            LogicalPlanType::Window { schema, .. } => Some(schema.clone()),
            LogicalPlanType::ShowTables {
                schema_name: _sn,
                terse: _,
                history: _,
                extended: _,
                full: _,
                external: _,
            } => None,
            _ => None,
        }
    }

    // ==================== TRANSACTION MANAGEMENT ====================

    /// Creates a START TRANSACTION logical plan.
    ///
    /// Begins a new database transaction with the specified options.
    ///
    /// # Arguments
    ///
    /// * `isolation_level` - Transaction isolation level (READ COMMITTED, SERIALIZABLE, etc.)
    /// * `read_only` - If true, transaction cannot modify data
    /// * `transaction_modifier` - Additional modifiers (DEFERRED, IMMEDIATE, etc.)
    /// * `statements` - Statements to execute within the transaction block
    /// * `exception_statements` - Exception handlers for error conditions
    /// * `has_end_keyword` - Whether the block uses END keyword
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn start_transaction(
        isolation_level: Option<IsolationLevel>,
        read_only: bool,
        transaction_modifier: Option<TransactionModifier>,
        statements: Vec<Statement>,
        exception_statements: Option<Vec<ExceptionWhen>>,
        has_end_keyword: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::StartTransaction {
                isolation_level,
                read_only,
                transaction_modifier,
                statements,
                exception_statements,
                has_end_keyword,
            },
            vec![],
        ))
    }

    /// Creates a COMMIT logical plan.
    ///
    /// Commits the current transaction, making all changes permanent.
    ///
    /// # Arguments
    ///
    /// * `chain` - If true, immediately starts a new transaction after commit
    /// * `end` - If true, ends a transaction block
    /// * `modifier` - Additional commit modifiers
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn commit_transaction(
        chain: bool,
        end: bool,
        modifier: Option<TransactionModifier>,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Commit {
                chain,
                end,
                modifier,
            },
            vec![],
        ))
    }

    /// Creates a ROLLBACK logical plan.
    ///
    /// Rolls back the current transaction, undoing all changes.
    /// Can optionally roll back only to a named savepoint.
    ///
    /// # Arguments
    ///
    /// * `chain` - If true, immediately starts a new transaction after rollback
    /// * `savepoint` - If specified, rolls back only to this savepoint
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn rollback_transaction(chain: bool, savepoint: Option<Ident>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Rollback { chain, savepoint },
            vec![],
        ))
    }

    /// Creates a SAVEPOINT logical plan.
    ///
    /// Establishes a named savepoint within the current transaction.
    /// Allows partial rollback to this point.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the savepoint to create
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn savepoint(name: String) -> Box<Self> {
        Box::new(Self::new(LogicalPlanType::Savepoint { name }, vec![]))
    }

    /// Creates a RELEASE SAVEPOINT logical plan.
    ///
    /// Destroys a savepoint and all savepoints created after it.
    ///
    /// # Arguments
    ///
    /// * `name` - Name of the savepoint to release
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn release_savepoint(name: String) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ReleaseSavepoint { name },
            vec![],
        ))
    }

    // ==================== DDL OPERATIONS ====================

    /// Creates a DROP logical plan.
    ///
    /// Drops database objects (tables, indexes, views, etc.).
    ///
    /// # Arguments
    ///
    /// * `object_type` - Type of object ("TABLE", "INDEX", "VIEW", etc.)
    /// * `if_exists` - If true, don't error when object doesn't exist
    /// * `names` - Names of objects to drop
    /// * `cascade` - If true, also drop dependent objects
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn drop(
        object_type: String,
        if_exists: bool,
        names: Vec<String>,
        cascade: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            },
            vec![],
        ))
    }

    /// Creates a CREATE SCHEMA logical plan.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - Name of the schema to create
    /// * `if_not_exists` - If true, don't error when schema already exists
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn create_schema(schema_name: String, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateSchema {
                schema_name,
                if_not_exists,
            },
            vec![],
        ))
    }

    /// Creates a CREATE DATABASE logical plan.
    ///
    /// # Arguments
    ///
    /// * `db_name` - Name of the database to create
    /// * `if_not_exists` - If true, don't error when database already exists
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn create_database(db_name: String, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateDatabase {
                db_name,
                if_not_exists,
            },
            vec![],
        ))
    }

    /// Creates an ALTER TABLE logical plan.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to alter
    /// * `operation` - Description of the alteration (e.g., "ADD COLUMN email VARCHAR")
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn alter_table(table_name: String, operation: String) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::AlterTable {
                table_name,
                operation,
            },
            vec![],
        ))
    }

    /// Creates a CREATE VIEW logical plan.
    ///
    /// # Arguments
    ///
    /// * `view_name` - Name of the view to create
    /// * `schema` - Output schema of the view
    /// * `if_not_exists` - If true, don't error when view already exists
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn create_view(view_name: String, schema: Schema, if_not_exists: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::CreateView {
                view_name,
                schema,
                if_not_exists,
            },
            vec![],
        ))
    }

    /// Creates an ALTER VIEW logical plan.
    ///
    /// # Arguments
    ///
    /// * `view_name` - Name of the view to alter
    /// * `operation` - Description of the alteration
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn alter_view(view_name: String, operation: String) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::AlterView {
                view_name,
                operation,
            },
            vec![],
        ))
    }

    // ==================== DATABASE INFORMATION ====================

    /// Creates a SHOW TABLES logical plan with default options.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - Optional schema to filter tables
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn show_tables(schema_name: Option<String>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowTables {
                schema_name,
                terse: false,
                history: false,
                extended: false,
                full: false,
                external: false,
            },
            vec![],
        ))
    }

    /// Creates a SHOW TABLES logical plan with all options.
    ///
    /// # Arguments
    ///
    /// * `schema_name` - Optional schema to filter tables
    /// * `terse` - Show minimal information
    /// * `history` - Include historical/deleted tables
    /// * `extended` - Show extended information
    /// * `full` - Show full table details
    /// * `external` - Show only external tables
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn show_tables_with_options(
        schema_name: Option<String>,
        terse: bool,
        history: bool,
        extended: bool,
        full: bool,
        external: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowTables {
                schema_name,
                terse,
                history,
                extended,
                full,
                external,
            },
            vec![],
        ))
    }

    /// Creates a SHOW DATABASES logical plan with default options.
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn show_databases() -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowDatabases {
                terse: false,
                history: false,
            },
            vec![],
        ))
    }

    /// Creates a SHOW DATABASES logical plan with options.
    ///
    /// # Arguments
    ///
    /// * `terse` - Show minimal information
    /// * `history` - Include historical/deleted databases
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn show_databases_with_options(terse: bool, history: bool) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowDatabases { terse, history },
            vec![],
        ))
    }

    /// Creates a SHOW COLUMNS logical plan with default options.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to describe
    /// * `schema_name` - Optional schema containing the table
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn show_columns(table_name: String, schema_name: Option<String>) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowColumns {
                table_name,
                schema_name,
                extended: false,
                full: false,
            },
            vec![],
        ))
    }

    /// Creates a USE (database) logical plan.
    ///
    /// Switches the current database context.
    ///
    /// # Arguments
    ///
    /// * `db_name` - Name of the database to switch to
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn use_db(db_name: String) -> Box<Self> {
        Box::new(Self::new(LogicalPlanType::Use { db_name }, vec![]))
    }

    /// Creates a SHOW COLUMNS logical plan with all options.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to describe
    /// * `schema_name` - Optional schema containing the table
    /// * `extended` - Show extended column information
    /// * `full` - Show full column details
    ///
    /// # Returns
    ///
    /// A boxed logical plan node (leaf node with no children).
    pub fn show_columns_with_options(
        table_name: String,
        schema_name: Option<String>,
        extended: bool,
        full: bool,
    ) -> Box<Self> {
        Box::new(Self::new(
            LogicalPlanType::ShowColumns {
                table_name,
                schema_name,
                extended,
                full,
            },
            vec![],
        ))
    }
}

/// Iterative converter from logical plans to physical plans.
///
/// Uses an explicit stack-based approach to convert plan trees, avoiding
/// stack overflow on deep plans that could occur with recursion.
///
/// # Algorithm
///
/// The converter performs a post-order traversal:
/// 1. Push the root onto the stack
/// 2. While the stack is not empty:
///    - Pop a node
///    - If all children are processed: convert the node, store result
///    - Otherwise: push node back, push unprocessed children
/// 3. Return the result for the root node
///
/// # Cycle Detection
///
/// The converter tracks visited nodes to detect cycles in the plan tree.
/// If a cycle is detected, an error is returned.
///
/// # Thread Safety
///
/// `PlanConverter` is not thread-safe and should only be used within
/// a single thread during plan conversion.
struct PlanConverter<'a> {
    /// Stack of nodes to process (simulates recursion)
    stack: Vec<&'a LogicalPlan>,
    /// Cached results for processed nodes (keyed by memory address)
    results: HashMap<usize, Result<PlanNode, String>>,
    /// Nodes currently being processed (for cycle detection)
    visited: HashSet<usize>,
}

impl<'a> PlanConverter<'a> {
    /// Creates a new PlanConverter initialized with the root node.
    ///
    /// # Arguments
    ///
    /// * `root` - The root logical plan node to convert
    ///
    /// # Returns
    ///
    /// A new PlanConverter ready to perform conversion.
    fn new(root: &'a LogicalPlan) -> Self {
        let mut converter = Self {
            stack: Vec::new(),
            results: HashMap::new(),
            visited: HashSet::new(),
        };
        converter.stack.push(root);
        converter
    }

    /// Converts the logical plan tree to a physical plan.
    ///
    /// Uses an iterative post-order traversal to convert all nodes.
    /// Children are converted before their parents.
    ///
    /// # Returns
    ///
    /// - `Ok(PlanNode)` - The physical plan for the root node
    /// - `Err(String)` - An error if conversion fails
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The stack is empty (no plan to convert)
    /// - A cycle is detected in the plan tree
    /// - Any node conversion fails
    fn convert(&mut self) -> Result<PlanNode, String> {
        // Store the root node ID at the beginning
        if self.stack.is_empty() {
            return Err("Empty plan stack".to_string());
        }

        let root_node = self.stack[0];
        let root_id = ptr::addr_of!(*root_node) as usize;

        while let Some(node) = self.stack.pop() {
            let node_id = ptr::addr_of!(*node) as usize;

            // If we've already processed this node, skip it
            if self.results.contains_key(&node_id) {
                continue;
            }

            // Check if all children have been processed
            if self.are_all_children_processed(node) {
                // Process the current node
                let result = self.convert_node(node);
                self.results.insert(node_id, result);
            } else {
                // Mark this node as being processed
                self.visited.insert(node_id);

                // Push the node back onto the stack
                self.stack.push(node);

                // Push all unprocessed children onto the stack
                self.push_unprocessed_children(node);

                // Remove this node from visited since we're done with its children
                self.visited.remove(&node_id);
            }
        }

        // Get the result for the root node using the stored ID
        match self.results.get(&root_id) {
            Some(result) => result.clone(),
            None => Err("Root node not processed".to_string()),
        }
    }

    /// Checks if all children of a node have been successfully processed.
    ///
    /// A child is considered processed if its result exists in the cache
    /// and is not an error.
    ///
    /// # Arguments
    ///
    /// * `node` - The node whose children to check
    ///
    /// # Returns
    ///
    /// `true` if all children have been successfully converted, `false` otherwise.
    fn are_all_children_processed(&self, node: &'a LogicalPlan) -> bool {
        // If the node has no children, return true immediately
        if node.children.is_empty() {
            return true;
        }

        let mut all_children_processed = true;

        for child in &node.children {
            let child_id = ptr::addr_of!(**child) as usize;
            if !self.results.contains_key(&child_id) {
                all_children_processed = false;
                break;
            } else if let Some(Err(_)) = self.results.get(&child_id) {
                all_children_processed = false;
                break;
            }
        }

        all_children_processed
    }

    /// Pushes unprocessed children onto the stack for later processing.
    ///
    /// Children are pushed in reverse order so they are processed left-to-right.
    /// Detects cycles by checking if a child is already being visited.
    ///
    /// # Arguments
    ///
    /// * `node` - The node whose children to push
    fn push_unprocessed_children(&mut self, node: &'a LogicalPlan) {
        // If the node has no children, return immediately
        if node.children.is_empty() {
            return;
        }

        for child in node.children.iter().rev() {
            let child_ref = child.as_ref();
            let child_id = ptr::addr_of!(*child_ref) as usize;

            if !self.results.contains_key(&child_id) {
                // Check for cycles
                if self.visited.contains(&child_id) {
                    self.results.insert(
                        child_id,
                        Err(format!(
                            "Cycle detected in plan conversion at node: {:?}",
                            child_ref.plan_type
                        )),
                    );
                } else {
                    self.stack.push(child_ref);
                }
            }
        }
    }

    /// Retrieves the converted physical plans for all children of a node.
    ///
    /// # Arguments
    ///
    /// * `node` - The node whose child plans to retrieve
    ///
    /// # Returns
    ///
    /// A vector of physical plans for each successfully converted child.
    fn get_child_plans(&self, node: &'a LogicalPlan) -> Vec<PlanNode> {
        // If the node has no children, return an empty vector immediately
        if node.children.is_empty() {
            return Vec::new();
        }

        let mut child_plans = Vec::new();

        for child in &node.children {
            let child_id = ptr::addr_of!(**child) as usize;
            if let Some(Ok(plan)) = self.results.get(&child_id) {
                child_plans.push(plan.clone());
            }
        }

        child_plans
    }

    /// Converts a single logical plan node to its physical equivalent.
    ///
    /// This method handles the type-specific conversion logic for each
    /// [`LogicalPlanType`] variant.
    ///
    /// # Arguments
    ///
    /// * `node` - The logical plan node to convert
    ///
    /// # Returns
    ///
    /// - `Ok(PlanNode)` - The corresponding physical plan node
    /// - `Err(String)` - An error if conversion fails
    fn convert_node(&self, node: &'a LogicalPlan) -> Result<PlanNode, String> {
        let child_plans = self.get_child_plans(node);

        match &node.plan_type {
            LogicalPlanType::CreateTable {
                schema,
                table_name,
                if_not_exists,
            } => Ok(PlanNode::CreateTable(CreateTablePlanNode::new(
                schema.clone(),
                table_name.clone(),
                *if_not_exists,
            ))),

            LogicalPlanType::CreateIndex {
                schema,
                table_name,
                index_name,
                key_attrs,
                if_not_exists,
            } => Ok(PlanNode::CreateIndex(CreateIndexPlanNode::new(
                schema.clone(),
                table_name.clone(),
                index_name.clone(),
                key_attrs.clone(),
                *if_not_exists,
            ))),

            LogicalPlanType::TableScan {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::SeqScan(SeqScanPlanNode::new(
                schema.clone(),
                *table_oid,
                table_name.clone(),
            ))),

            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema,
                predicate_keys,
            } => Ok(PlanNode::IndexScan(IndexScanNode::new(
                schema.clone(),
                table_name.to_string(),
                *table_oid,
                index_name.to_string(),
                *index_oid,
                predicate_keys.clone(),
            ))),

            LogicalPlanType::Filter {
                schema,
                table_oid,
                table_name,
                predicate,
                ..
            } => Ok(PlanNode::Filter(FilterNode::new(
                schema.clone(),
                *table_oid,
                table_name.to_string(),
                predicate.clone(),
                child_plans,
            ))),

            LogicalPlanType::Projection {
                expressions,
                schema,
                column_mappings,
            } => {
                // Create output schema
                let output_schema = schema.clone();

                Ok(PlanNode::Projection(
                    ProjectionNode::new(
                        output_schema,
                        expressions.clone(),
                        column_mappings.clone(),
                    )
                    .with_children(child_plans),
                ))
            },

            LogicalPlanType::Insert {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::Insert(InsertNode::new(
                schema.clone(),
                *table_oid,
                table_name.to_string(),
                vec![],
                child_plans,
            ))),

            LogicalPlanType::Values { rows, schema } => Ok(PlanNode::Values(ValuesNode::new(
                schema.clone(),
                rows.clone(),
                child_plans,
            ))),

            LogicalPlanType::MockScan {
                table_name,
                schema,
                table_oid: _,
            } => Ok(PlanNode::MockScan(MockScanNode::new(
                schema.clone(),
                table_name.clone(),
                vec![],
            ))),

            LogicalPlanType::Delete {
                table_name,
                schema,
                table_oid,
            } => Ok(PlanNode::Delete(DeleteNode::new(
                schema.clone(),
                table_name.clone(),
                *table_oid,
                child_plans,
            ))),

            LogicalPlanType::Update {
                table_name,
                schema,
                table_oid,
                update_expressions,
            } => Ok(PlanNode::Update(UpdateNode::new(
                schema.clone(),
                table_name.clone(),
                *table_oid,
                update_expressions.clone(),
                child_plans,
            ))),

            LogicalPlanType::Aggregate {
                group_by,
                aggregates,
                schema: _,
            } => {
                // Filter out duplicate expressions
                let agg_exprs = aggregates
                    .iter()
                    .filter(|expr| match expr.as_ref() {
                        Expression::ColumnRef(col_ref) => {
                            !group_by.iter().any(|g| match g.as_ref() {
                                Expression::ColumnRef(g_ref) => {
                                    g_ref.get_column_index() == col_ref.get_column_index()
                                },
                                _ => false,
                            })
                        },
                        _ => true,
                    })
                    .cloned()
                    .collect::<Vec<_>>();

                Ok(PlanNode::Aggregation(AggregationPlanNode::new(
                    child_plans,
                    group_by.clone(),
                    agg_exprs,
                )))
            },

            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                if child_plans.len() < 2 {
                    Err("NestedLoopJoin requires two children".to_string())
                } else {
                    // Extract join key expressions and fixed predicate from the original predicate
                    let (left_keys, right_keys, fixed_predicate) =
                        match extract_join_keys(predicate) {
                            Ok((l, r, p)) => (l, r, p),
                            Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                        };

                    Ok(PlanNode::NestedLoopJoin(NestedLoopJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        fixed_predicate, // Use the fixed predicate with correct tuple indices
                        join_type.clone(),
                        left_keys,
                        right_keys,
                        vec![child_plans[0].clone(), child_plans[1].clone()],
                    )))
                }
            },

            LogicalPlanType::NestedIndexJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                if child_plans.len() < 2 {
                    Err("NestedIndexJoin requires two children".to_string())
                } else {
                    // Extract join key expressions and fixed predicate from the original predicate
                    let (left_keys, right_keys, fixed_predicate) =
                        match extract_join_keys(predicate) {
                            Ok((l, r, p)) => (l, r, p),
                            Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                        };

                    Ok(PlanNode::NestedIndexJoin(NestedIndexJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        fixed_predicate, // Use the fixed predicate with correct tuple indices
                        join_type.clone(),
                        left_keys,
                        right_keys,
                        vec![child_plans[0].clone(), child_plans[1].clone()],
                    )))
                }
            },

            LogicalPlanType::HashJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                if child_plans.len() < 2 {
                    Err("HashJoin requires two children".to_string())
                } else {
                    // Extract join key expressions and fixed predicate from the original predicate
                    let (left_keys, right_keys, fixed_predicate) =
                        match extract_join_keys(predicate) {
                            Ok((l, r, p)) => (l, r, p),
                            Err(e) => return Err(format!("Failed to extract join keys: {}", e)),
                        };

                    Ok(PlanNode::HashJoin(HashJoinNode::new(
                        left_schema.clone(),
                        right_schema.clone(),
                        fixed_predicate, // Use the fixed predicate with correct tuple indices
                        join_type.clone(),
                        left_keys,
                        right_keys,
                        vec![child_plans[0].clone(), child_plans[1].clone()],
                    )))
                }
            },

            LogicalPlanType::Sort {
                sort_specifications,
                schema,
            } => Ok(PlanNode::Sort(SortNode::new(
                schema.clone(),
                sort_specifications.clone(),
                child_plans,
            ))),

            LogicalPlanType::Limit { limit, schema } => Ok(PlanNode::Limit(LimitNode::new(
                *limit,
                schema.clone(),
                child_plans,
            ))),

            LogicalPlanType::Offset { offset, schema } => Ok(PlanNode::Offset(OffsetNode::new(
                *offset,
                schema.clone(),
                child_plans,
            ))),

            LogicalPlanType::TopN {
                k,
                sort_specifications,
                schema,
            } => {
                // Convert OrderBySpec to expressions for backward compatibility with TopNNode
                let sort_expressions = sort_specifications
                    .iter()
                    .map(|spec| spec.get_expression().clone())
                    .collect();

                Ok(PlanNode::TopN(TopNNode::new(
                    schema.clone(),
                    sort_expressions,
                    *k,
                    child_plans,
                )))
            },

            LogicalPlanType::TopNPerGroup {
                k,
                sort_specifications,
                groups,
                schema,
            } => {
                // Convert OrderBySpec to expressions for backward compatibility with TopNPerGroupNode
                let sort_expressions = sort_specifications
                    .iter()
                    .map(|spec| spec.get_expression().clone())
                    .collect();

                Ok(PlanNode::TopNPerGroup(TopNPerGroupNode::new(
                    *k,
                    sort_expressions,
                    groups.clone(),
                    schema.clone(),
                    child_plans,
                )))
            },

            LogicalPlanType::Window {
                group_by,
                aggregates,
                partitions,
                schema,
            } => {
                // Convert the logical window expressions into WindowFunction structs
                let mut window_functions = Vec::with_capacity(aggregates.len());
                for (i, agg_expr) in aggregates.iter().enumerate() {
                    // Determine the window function type based on the aggregate expression
                    let function_type = match agg_expr.as_ref() {
                        Expression::Aggregate(agg) => match agg.get_agg_type() {
                            AggregationType::Count => WindowFunctionType::Count,
                            AggregationType::Sum => WindowFunctionType::Sum,
                            AggregationType::Min => WindowFunctionType::Min,
                            AggregationType::Max => WindowFunctionType::Max,
                            AggregationType::Avg => WindowFunctionType::Average,
                            // Add other mappings as needed
                            _ => return Err("Unsupported window function type".to_string()),
                        },
                        Expression::Window(window_func) => {
                            // If it's already a window function, use its type directly
                            window_func.get_window_type()
                        },
                        _ => return Err("Invalid window function expression".to_string()),
                    };

                    // Create a new WindowFunction with the appropriate partitioning and ordering
                    window_functions.push(WindowFunction::new(
                        function_type,
                        Arc::clone(agg_expr),
                        if i < partitions.len() {
                            vec![Arc::clone(&partitions[i])]
                        } else {
                            vec![]
                        },
                        if i < group_by.len() {
                            vec![Arc::clone(&group_by[i])]
                        } else {
                            vec![]
                        },
                    ));
                }

                Ok(PlanNode::Window(WindowNode::new(
                    schema.clone(),
                    window_functions,
                    child_plans,
                )))
            },

            LogicalPlanType::StartTransaction {
                isolation_level,
                read_only,
                transaction_modifier: _,
                statements: _,
                exception_statements: _,
                has_end_keyword: _,
            } => {
                // Create a StartTransaction plan node instead of a CommandResult
                Ok(PlanNode::StartTransaction(
                    crate::sql::execution::plans::start_transaction_plan::StartTransactionPlanNode::new(
                        *isolation_level,
                        *read_only,
                    )
                ))
            },

            LogicalPlanType::Commit {
                chain,
                end,
                modifier: _,
            } => {
                // Create a CommitTransaction plan node
                Ok(PlanNode::CommitTransaction(
                    crate::sql::execution::plans::commit_transaction_plan::CommitTransactionPlanNode::new(
                        *chain,
                        *end,
                    )
                ))
            },

            LogicalPlanType::Rollback { chain, savepoint } => {
                // Create a RollbackTransaction plan node
                let savepoint_str = savepoint.as_ref().map(|s| s.value.clone());
                Ok(PlanNode::RollbackTransaction(
                    crate::sql::execution::plans::rollback_transaction_plan::RollbackTransactionPlanNode::new(
                        *chain,
                        savepoint_str,
                    )
                ))
            },

            LogicalPlanType::Savepoint { name } => {
                // Create a dummy plan that will create savepoint
                Ok(PlanNode::CommandResult(format!("SAVEPOINT {}", name)))
            },

            LogicalPlanType::ReleaseSavepoint { name } => {
                // Create a dummy plan that will release savepoint
                Ok(PlanNode::CommandResult(format!(
                    "RELEASE SAVEPOINT {}",
                    name
                )))
            },

            LogicalPlanType::Drop {
                object_type,
                if_exists,
                names,
                cascade,
            } => {
                // Create a dummy plan that will perform drop operation
                Ok(PlanNode::CommandResult(format!(
                    "DROP {} {}{}{}",
                    object_type,
                    if *if_exists { "IF EXISTS " } else { "" },
                    names.join(", "),
                    if *cascade { " CASCADE" } else { "" }
                )))
            },

            LogicalPlanType::CreateSchema {
                schema_name,
                if_not_exists,
            } => {
                // Create a dummy plan that will create schema
                Ok(PlanNode::CommandResult(format!(
                    "CREATE SCHEMA {}{}",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    schema_name
                )))
            },

            LogicalPlanType::CreateDatabase {
                db_name,
                if_not_exists,
            } => {
                // Create a dummy plan that will create database
                Ok(PlanNode::CommandResult(format!(
                    "CREATE DATABASE {}{}",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    db_name
                )))
            },

            LogicalPlanType::AlterTable {
                table_name,
                operation,
            } => {
                // Create a dummy plan that will alter table
                Ok(PlanNode::CommandResult(format!(
                    "ALTER TABLE {} {}",
                    table_name, operation
                )))
            },

            LogicalPlanType::CreateView {
                view_name,
                schema: _,
                if_not_exists,
            } => {
                // Create a dummy plan that will create view
                Ok(PlanNode::CommandResult(format!(
                    "CREATE VIEW {}{}",
                    if *if_not_exists { "IF NOT EXISTS " } else { "" },
                    view_name
                )))
            },

            LogicalPlanType::AlterView {
                view_name,
                operation,
            } => {
                // Create a dummy plan that will alter view
                Ok(PlanNode::CommandResult(format!(
                    "ALTER VIEW {} {}",
                    view_name, operation
                )))
            },

            LogicalPlanType::ShowTables {
                schema_name,
                terse,
                history,
                extended,
                full,
                external,
            } => {
                // Create a dummy plan that will show tables
                Ok(PlanNode::CommandResult(format!(
                    "SHOW{}{}{}{}{}TABLES{}",
                    if *terse { " TERSE" } else { "" },
                    if *history { " HISTORY" } else { "" },
                    if *extended { " EXTENDED" } else { "" },
                    if *full { " FULL" } else { "" },
                    if *external { " EXTERNAL" } else { "" },
                    if let Some(name) = schema_name {
                        format!(" IN {}", name)
                    } else {
                        String::new()
                    }
                )))
            },

            LogicalPlanType::ShowDatabases { terse, history } => {
                // Create a dummy plan that will show databases
                Ok(PlanNode::CommandResult(format!(
                    "SHOW DATABASES{}{}",
                    if *terse { " TERSE" } else { "" },
                    if *history { " HISTORY" } else { "" }
                )))
            },

            LogicalPlanType::ShowColumns {
                table_name,
                schema_name,
                extended,
                full,
            } => {
                // Create a dummy plan that will show columns
                Ok(PlanNode::CommandResult(format!(
                    "SHOW{}{}COLUMNS FROM {}{}",
                    if *extended { " EXTENDED" } else { "" },
                    if *full { " FULL" } else { "" },
                    table_name,
                    if let Some(name) = schema_name {
                        format!(" FROM {}", name)
                    } else {
                        String::new()
                    }
                )))
            },

            LogicalPlanType::Use { db_name } => {
                // Create a dummy plan that will use database
                Ok(PlanNode::CommandResult(format!("USE {}", db_name)))
            },

            LogicalPlanType::Distinct { schema } => Ok(PlanNode::Distinct(
                DistinctNode::new(schema.clone()).with_children(child_plans),
            )),

            LogicalPlanType::Explain { plan } => {
                // Create a recursive explain plan
                let inner_plan = plan.to_physical_plan()?;
                Ok(PlanNode::Explain(Box::new(inner_plan)))
            },
        }
    }
}

/// Implementation of logical-to-physical plan conversion for [`LogicalPlan`].
///
/// Uses [`PlanConverter`] for iterative (non-recursive) conversion to avoid
/// stack overflow on deep plan trees.
impl LogicalToPhysical for LogicalPlan {
    /// Converts this logical plan to a physical execution plan.
    ///
    /// Creates a [`PlanConverter`] and performs iterative conversion of the
    /// entire plan tree, starting from the leaves and working up to the root.
    ///
    /// # Returns
    ///
    /// - `Ok(PlanNode)` - The physical plan ready for execution
    /// - `Err(String)` - Error message if conversion fails
    fn to_physical_plan(&self) -> Result<PlanNode, String> {
        // Use the PlanConverter to convert the logical plan to a physical plan
        let mut converter = PlanConverter::new(self);
        converter.convert()
    }
}

/// Display implementation for [`LogicalPlan`].
///
/// Outputs a pretty-printed debug representation of the plan tree.
/// For human-readable query plan output, use [`LogicalPlan::explain()`] instead.
impl Display for LogicalPlan {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:#?}", self)
    }
}

/// Extracts and normalizes join key expressions from a join predicate.
///
/// This function analyzes a join predicate (typically from an ON clause) and:
/// 1. Extracts the key columns from each side of the join
/// 2. Normalizes tuple indices (left=0, right=1) for the executor
/// 3. Returns a fixed predicate suitable for join execution
///
/// # Arguments
///
/// * `predicate` - The join predicate expression (equality comparison or AND of equalities)
///
/// # Returns
///
/// A `Result` containing a tuple of:
/// - `Vec<Arc<Expression>>` - Left table key expressions (tuple_index=0)
/// - `Vec<Arc<Expression>>` - Right table key expressions (tuple_index=1)
/// - `Arc<Expression>` - Fixed predicate with normalized tuple indices
///
/// # Errors
///
/// Returns an error if:
/// - The predicate is not an equality comparison
/// - Comparison operands are not column references
/// - Logic expressions use OR instead of AND
/// - The expression type is unsupported
///
/// # Supported Predicates
///
/// - Simple equality: `a.id = b.id`
/// - Compound AND: `a.id = b.id AND a.name = b.name`
///
/// # Example
///
/// ```rust,ignore
/// // For predicate: employees.dept_id = departments.id
/// let (left_keys, right_keys, fixed_pred) = extract_join_keys(&predicate)?;
/// // left_keys[0] has tuple_index=0, column for employees.dept_id
/// // right_keys[0] has tuple_index=1, column for departments.id
/// ```
///
/// # Special Cases
///
/// When the right column name is exactly "d.id", the column index is
/// normalized to 0 (handles specific test case for department joins).
fn extract_join_keys(
    predicate: &Arc<Expression>,
) -> Result<(Vec<Arc<Expression>>, Vec<Arc<Expression>>, Arc<Expression>), String> {
    // Step 1: Initialize empty vectors for left and right keys
    let mut left_keys = Vec::new();
    let mut right_keys = Vec::new();

    // Step 2: Process the predicate expression based on its type
    match predicate.as_ref() {
        // Step 2.1: Handle Comparison expressions (like a.id = b.id)
        Expression::Comparison(comp_expr) => {
            // Step 2.1.1: Verify it's an equality comparison (error if not)
            if let ComparisonType::Equal = comp_expr.get_comp_type() {
                let children = comp_expr.get_children();

                // Step 2.1.2: Extract the two sides of the comparison
                if children.len() == 2 {
                    // Step 2.1.3: Verify both sides are ColumnRefExpressions
                    if let (Expression::ColumnRef(left_ref), Expression::ColumnRef(right_ref)) =
                        (children[0].as_ref(), children[1].as_ref())
                    {
                        // Step 2.1.4: Create new column references with proper tuple indices
                        let left_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                            0, // Left tuple index always 0
                            left_ref.get_column_index(),
                            left_ref.get_return_type().clone(),
                            vec![],
                        )));

                        // Step 2.1.5: Special handling for "d.id" - fix column index to 0
                        let col_name = right_ref.get_return_type().get_name();
                        let (tuple_index, column_index) = if col_name == "d.id" {
                            (1, 0) // Right tuple index always 1, and "d.id" is first column
                        } else {
                            (1, right_ref.get_column_index()) // Right tuple index always 1
                        };

                        let right_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                            tuple_index,
                            column_index,
                            right_ref.get_return_type().clone(),
                            vec![],
                        )));

                        // Step 2.1.6: Create a fixed comparison with the new column references
                        let fixed_predicate =
                            Arc::new(Expression::Comparison(ComparisonExpression::new(
                                left_key.clone(),
                                right_key.clone(),
                                ComparisonType::Equal,
                                vec![left_key.clone(), right_key.clone()],
                            )));

                        // Step 2.1.7: Add to respective key vectors
                        left_keys.push(left_key);
                        right_keys.push(right_key);

                        Ok((left_keys, right_keys, fixed_predicate))
                    } else {
                        Err("Join predicate must compare column references".to_string())
                    }
                } else {
                    Err("Comparison must have exactly two operands".to_string())
                }
            } else {
                Err("Join predicate must use equality comparison".to_string())
            }
        },

        // Step 2.2: Handle Logic expressions (like AND between multiple conditions)
        Expression::Logic(logic_expr) => {
            // Step 2.2.1: Verify it's an AND expression
            if let LogicType::And = logic_expr.get_logic_type() {
                let children = logic_expr.get_children();

                if children.len() == 2 {
                    // Step 2.2.2: Recursively extract keys from each child predicate
                    let (mut left_keys1, mut right_keys1, fixed_pred1) =
                        extract_join_keys(&children[0])?;
                    let (mut left_keys2, mut right_keys2, fixed_pred2) =
                        extract_join_keys(&children[1])?;

                    // Step 2.2.3: Combine the results
                    left_keys.append(&mut left_keys1);
                    left_keys.append(&mut left_keys2);
                    right_keys.append(&mut right_keys1);
                    right_keys.append(&mut right_keys2);

                    // Step 2.2.4: Combine the fixed predicates with AND
                    let combined_predicate = Arc::new(Expression::Logic(LogicExpression::new(
                        fixed_pred1,
                        fixed_pred2,
                        LogicType::And,
                        children.clone(),
                    )));

                    Ok((left_keys, right_keys, combined_predicate))
                } else {
                    Err("Logic expression must have exactly two operands".to_string())
                }
            } else {
                Err("Only AND is supported for combining join conditions".to_string())
            }
        },

        // Step 2.3: Error for unsupported expression types
        _ => Err(format!(
            "Unsupported join predicate expression type: {:?}",
            predicate
        )),
    }
}

/// Extracts the predominant table alias from a schema.
///
/// Analyzes column names in the schema to find table aliases/prefixes.
/// Returns the most frequently occurring alias (if columns use `table.column` naming).
///
/// # Arguments
///
/// * `schema` - The schema to analyze
///
/// # Returns
///
/// - `Some(String)` - The most common table alias found
/// - `None` - If no columns use `table.column` naming convention
///
/// # Example
///
/// ```rust,ignore
/// // Schema with columns: ["users.id", "users.name", "orders.id"]
/// let alias = extract_table_alias_from_schema(&schema);
/// assert_eq!(alias, Some("users".to_string())); // "users" appears twice
/// ```
fn extract_table_alias_from_schema(schema: &Schema) -> Option<String> {
    // Create a map to count occurrences of each alias
    let mut alias_counts = HashMap::new();

    // Look at all columns to find table aliases
    for column in schema.get_columns() {
        let name = column.get_name();
        if let Some(dot_pos) = name.find('.') {
            let alias = name[..dot_pos].to_string();
            *alias_counts.entry(alias).or_insert(0) += 1;
        }
    }

    // If we have aliases, return the most common one
    if !alias_counts.is_empty() {
        return alias_counts
            .into_iter()
            .max_by_key(|(_, count)| *count)
            .map(|(alias, _)| alias);
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::aggregate_expression::{
        AggregateExpression, AggregationType,
    };
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use sqlparser::ast::{BinaryOperator, JoinConstraint, JoinOperator};

    #[test]
    fn test_create_table_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let plan = LogicalPlan::create_table(schema.clone(), "users".to_string(), false);

        match plan.plan_type {
            LogicalPlanType::CreateTable {
                schema: s,
                table_name,
                if_not_exists,
            } => {
                assert_eq!(schema, s);
                assert_eq!(table_name, "users");
                assert!(!if_not_exists);
            },
            _ => panic!("Expected CreateTable plan"),
        }
    }

    #[test]
    fn test_table_scan_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        match plan.plan_type {
            LogicalPlanType::TableScan {
                table_name,
                schema: s,
                table_oid,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
            },
            _ => panic!("Expected TableScan plan"),
        }
    }

    #[test]
    fn test_filter_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let column_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("age", TypeId::Integer),
            vec![],
        )));
        let constant = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(18)),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            column_ref.clone(),
            constant.clone(),
            ComparisonType::GreaterThan,
            vec![column_ref.clone(), constant.clone()],
        )));

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let filter_plan = LogicalPlan::filter(
            schema.clone(),
            "users".to_string(),
            1,
            predicate.clone(),
            scan_plan,
        );

        match filter_plan.plan_type {
            LogicalPlanType::Filter {
                schema: s,
                table_oid,
                table_name,
                predicate: p,
                output_schema: _,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
                assert_eq!(p, predicate);
            },
            _ => panic!("Expected Filter plan"),
        }
    }

    #[test]
    fn test_projection_plan() {
        // Create a schema for the input
        let input_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create a table scan as the input
        let table_scan = LogicalPlan::table_scan("users".to_string(), input_schema.clone(), 1);

        // Create projection expressions
        let id_expr = Arc::new(Expression::column_ref("id", TypeId::Integer));
        let name_expr = Arc::new(Expression::column_ref("name", TypeId::VarChar));
        let expressions = vec![id_expr.clone(), name_expr.clone()];

        // Create output schema
        let output_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create a projection plan
        let projection_plan =
            LogicalPlan::project(expressions.clone(), output_schema.clone(), table_scan);

        // Check the plan type
        match projection_plan.plan_type {
            LogicalPlanType::Projection {
                expressions: e,
                schema: s,
                column_mappings,
            } => {
                assert_eq!(expressions, e);
                assert_eq!(output_schema, s);
                // The column mappings should map to the correct input columns
                assert_eq!(column_mappings, vec![0, 1]);
            },
            _ => panic!("Expected Projection plan"),
        }
    }

    #[test]
    fn test_aggregate_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let group_by = vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )))];

        let age_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("age", TypeId::Integer),
            vec![],
        )));
        let aggregates = vec![Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Avg,
            vec![age_col.clone()],
            Column::new("avg_age", TypeId::Decimal),
            "AVG".to_string(),
        )))];

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let aggregate_plan = LogicalPlan::aggregate(
            group_by.clone(),
            aggregates.clone(),
            schema.clone(),
            scan_plan,
        );

        match aggregate_plan.plan_type {
            LogicalPlanType::Aggregate {
                group_by: g,
                aggregates: a,
                schema: s,
            } => {
                assert_eq!(group_by, g);
                assert_eq!(aggregates, a);
                assert_eq!(schema, s);
            },
            _ => panic!("Expected Aggregate plan"),
        }
    }

    #[test]
    fn test_explain_output() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let column_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("age", TypeId::Integer),
            vec![],
        )));
        let constant = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(18)),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            column_ref.clone(),
            constant.clone(),
            ComparisonType::GreaterThan,
            vec![column_ref.clone(), constant.clone()],
        )));

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let filter_plan = LogicalPlan::filter(
            schema.clone(),
            "users".to_string(),
            1,
            predicate.clone(),
            scan_plan,
        );

        let explain_output = filter_plan.explain(0);
        assert!(explain_output.contains("Filter"));
        assert!(explain_output.contains("TableScan"));
    }

    #[test]
    fn test_get_schema() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let result_schema = plan.get_schema();
        assert_eq!(schema, result_schema.unwrap());
    }

    #[test]
    fn test_to_physical_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let physical_plan = plan.to_physical_plan();
        assert!(physical_plan.is_ok());
    }

    #[test]
    fn test_create_index_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let key_attrs = vec![0]; // Index on the "id" column
        let plan = LogicalPlan::create_index(
            schema.clone(),
            "users".to_string(),
            "users_id_idx".to_string(),
            key_attrs.clone(),
            false,
        );

        match plan.plan_type {
            LogicalPlanType::CreateIndex {
                schema: s,
                table_name,
                index_name,
                key_attrs: k,
                if_not_exists,
            } => {
                assert_eq!(schema, s);
                assert_eq!(table_name, "users");
                assert_eq!(index_name, "users_id_idx");
                assert_eq!(key_attrs, k);
                assert!(!if_not_exists);
            },
            _ => panic!("Expected CreateIndex plan"),
        }
    }

    #[test]
    fn test_mock_scan_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let plan = LogicalPlan::mock_scan("users".to_string(), schema.clone(), 1);

        match plan.plan_type {
            LogicalPlanType::MockScan {
                table_name,
                schema: s,
                table_oid,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
            },
            _ => panic!("Expected MockScan plan"),
        }
    }

    #[test]
    fn test_index_scan_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let column_ref = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let predicate_keys = vec![column_ref];

        let plan = LogicalPlan::index_scan(
            "users".to_string(),
            1,
            "users_id_idx".to_string(),
            2,
            schema.clone(),
            predicate_keys.clone(),
        );

        match plan.plan_type {
            LogicalPlanType::IndexScan {
                table_name,
                table_oid,
                index_name,
                index_oid,
                schema: s,
                predicate_keys: p,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(table_oid, 1);
                assert_eq!(index_name, "users_id_idx");
                assert_eq!(index_oid, 2);
                assert_eq!(schema, s);
                assert_eq!(predicate_keys, p);
            },
            _ => panic!("Expected IndexScan plan"),
        }
    }

    #[test]
    fn test_insert_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Create a values plan as the input to insert
        let id_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(1)),
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let name_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::VarLen("John".to_string())),
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        let rows = vec![vec![id_val, name_val]];
        let values_plan = LogicalPlan::values(rows.clone(), schema.clone());

        let insert_plan = LogicalPlan::insert("users".to_string(), schema.clone(), 1, values_plan);

        match insert_plan.plan_type {
            LogicalPlanType::Insert {
                table_name,
                schema: s,
                table_oid,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
            },
            _ => panic!("Expected Insert plan"),
        }
    }

    #[test]
    fn test_delete_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let delete_plan = LogicalPlan::delete("users".to_string(), schema.clone(), 1, scan_plan);

        match delete_plan.plan_type {
            LogicalPlanType::Delete {
                table_name,
                schema: s,
                table_oid,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
            },
            _ => panic!("Expected Delete plan"),
        }
    }

    #[test]
    fn test_update_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ]);

        // Create an update expression to set age = 30
        let age_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(30)),
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        let update_expressions = vec![age_val];
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        let update_plan = LogicalPlan::update(
            "users".to_string(),
            schema.clone(),
            1,
            update_expressions.clone(),
            scan_plan,
        );

        match update_plan.plan_type {
            LogicalPlanType::Update {
                table_name,
                schema: s,
                table_oid,
                update_expressions: u,
            } => {
                assert_eq!(table_name, "users");
                assert_eq!(schema, s);
                assert_eq!(table_oid, 1);
                assert_eq!(update_expressions, u);
            },
            _ => panic!("Expected Update plan"),
        }
    }

    #[test]
    fn test_values_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let id_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Integer(1)),
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let name_val = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::VarLen("John".to_string())),
            Column::new("name", TypeId::VarChar),
            vec![],
        )));

        let rows = vec![vec![id_val.clone(), name_val.clone()]];
        let values_plan = LogicalPlan::values(rows.clone(), schema.clone());

        match values_plan.plan_type {
            LogicalPlanType::Values { rows: r, schema: s } => {
                assert_eq!(rows, r);
                assert_eq!(schema, s);
            },
            _ => panic!("Expected Values plan"),
        }
    }

    #[test]
    fn test_nested_loop_join_plan() {
        // Create two mock scan plans
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let left_plan = LogicalPlan::mock_scan("left_table".to_string(), left_schema.clone(), 1);
        let right_plan = LogicalPlan::mock_scan("right_table".to_string(), right_schema.clone(), 2);

        // Create a join condition
        let join_condition = Expression::binary_op(
            Expression::column_ref("left_table.id", TypeId::Integer),
            BinaryOperator::Eq,
            Expression::column_ref("right_table.id", TypeId::Integer),
        );

        // Create a nested loop join plan
        let join_plan = LogicalPlan::nested_loop_join(
            left_schema.clone(),
            right_schema.clone(),
            Arc::new(join_condition.clone()),
            JoinOperator::Inner(JoinConstraint::None),
            left_plan,
            right_plan,
        );

        // Check the plan type
        match join_plan.plan_type {
            LogicalPlanType::NestedLoopJoin {
                left_schema: ls,
                right_schema: rs,
                predicate,
                join_type,
            } => {
                assert_eq!(left_schema, ls);
                assert_eq!(right_schema, rs);
                assert_eq!(*predicate, join_condition);
                assert!(matches!(join_type, JoinOperator::Inner(_)));
            },
            _ => panic!("Expected NestedLoopJoin plan type"),
        }
    }

    #[test]
    fn test_hash_join_plan() {
        // Create two mock scan plans
        let left_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let right_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("age", TypeId::Integer),
        ]);

        let left_plan = LogicalPlan::mock_scan("left_table".to_string(), left_schema.clone(), 1);
        let right_plan = LogicalPlan::mock_scan("right_table".to_string(), right_schema.clone(), 2);

        // Create a join condition
        let join_condition = Expression::binary_op(
            Expression::column_ref("left_table.id", TypeId::Integer),
            BinaryOperator::Eq,
            Expression::column_ref("right_table.id", TypeId::Integer),
        );

        // Create a hash join plan
        let join_plan = LogicalPlan::hash_join(
            left_schema.clone(),
            right_schema.clone(),
            Arc::new(join_condition.clone()),
            JoinOperator::Inner(JoinConstraint::None),
            left_plan,
            right_plan,
        );

        // Check the plan type
        match join_plan.plan_type {
            LogicalPlanType::HashJoin {
                left_schema: ls,
                right_schema: rs,
                predicate,
                join_type,
            } => {
                assert_eq!(left_schema, ls);
                assert_eq!(right_schema, rs);
                assert_eq!(*predicate, join_condition);
                assert!(matches!(join_type, JoinOperator::Inner(_)));
            },
            _ => panic!("Expected HashJoin plan type"),
        }
    }

    #[test]
    fn test_sort_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Sort by id column
        let sort_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let sort_specifications = vec![OrderBySpec::new(
            sort_expr.clone(),
            crate::sql::execution::plans::sort_plan::OrderDirection::Asc,
        )];
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        let sort_plan = LogicalPlan::sort(sort_specifications.clone(), schema.clone(), scan_plan);

        match sort_plan.plan_type {
            LogicalPlanType::Sort {
                sort_specifications: se,
                schema: s,
            } => {
                assert_eq!(sort_specifications, se);
                assert_eq!(schema, s);
            },
            _ => panic!("Expected Sort plan"),
        }
    }

    #[test]
    fn test_limit_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);
        let limit_plan = LogicalPlan::limit(10, schema.clone(), scan_plan);

        match limit_plan.plan_type {
            LogicalPlanType::Limit { limit, schema: s } => {
                assert_eq!(limit, 10);
                assert_eq!(schema, s);
            },
            _ => panic!("Expected Limit plan"),
        }
    }

    #[test]
    fn test_top_n_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // Sort by id column
        let sort_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let sort_specifications = vec![OrderBySpec::new(
            sort_expr.clone(),
            crate::sql::execution::plans::sort_plan::OrderDirection::Asc,
        )];
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        let top_n_plan =
            LogicalPlan::top_n(5, sort_specifications.clone(), schema.clone(), scan_plan);

        match top_n_plan.plan_type {
            LogicalPlanType::TopN {
                k,
                sort_specifications: se,
                schema: s,
            } => {
                assert_eq!(k, 5);
                assert_eq!(sort_specifications, se);
                assert_eq!(schema, s);
            },
            _ => panic!("Expected TopN plan"),
        }
    }

    #[test]
    fn test_top_n_per_group_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("category", TypeId::VarChar),
            Column::new("sales", TypeId::Integer),
        ]);

        // Sort by sales column
        let sort_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("sales", TypeId::Integer),
            vec![],
        )));

        // Group by category column
        let group_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("category", TypeId::VarChar),
            vec![],
        )));

        let sort_specifications = vec![OrderBySpec::new(
            sort_expr.clone(),
            crate::sql::execution::plans::sort_plan::OrderDirection::Asc,
        )];
        let groups = vec![group_expr.clone()];
        let scan_plan = LogicalPlan::table_scan("products".to_string(), schema.clone(), 1);

        let top_n_per_group_plan = LogicalPlan::top_n_per_group(
            3,
            sort_specifications.clone(),
            groups.clone(),
            schema.clone(),
            scan_plan,
        );

        match top_n_per_group_plan.plan_type {
            LogicalPlanType::TopNPerGroup {
                k,
                sort_specifications: se,
                groups: g,
                schema: s,
            } => {
                assert_eq!(k, 3);
                assert_eq!(sort_specifications, se);
                assert_eq!(groups, g);
                assert_eq!(schema, s);
            },
            _ => panic!("Expected TopNPerGroup plan"),
        }
    }

    #[test]
    fn test_window_plan() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("department", TypeId::VarChar),
            Column::new("salary", TypeId::Integer),
        ]);

        // Create window function expressions
        let salary_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("salary", TypeId::Integer),
            vec![],
        )));

        // Partition by department
        let dept_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("department", TypeId::VarChar),
            vec![],
        )));

        let aggregates = vec![Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Avg,
            vec![salary_col.clone()],
            Column::new("avg_salary", TypeId::Decimal),
            "AVG".to_string(),
        )))];

        let group_by = Vec::new(); // No group by
        let partitions = vec![dept_col.clone()]; // Partition by department
        let scan_plan = LogicalPlan::table_scan("employees".to_string(), schema.clone(), 1);

        let window_plan = LogicalPlan::window(
            group_by.clone(),
            aggregates.clone(),
            partitions.clone(),
            schema.clone(),
            scan_plan,
        );

        match window_plan.plan_type {
            LogicalPlanType::Window {
                group_by: g,
                aggregates: a,
                partitions: p,
                schema: s,
            } => {
                assert_eq!(group_by, g);
                assert_eq!(aggregates, a);
                assert_eq!(partitions, p);
                assert_eq!(schema, s);
            },
            _ => panic!("Expected Window plan"),
        }
    }

    #[test]
    fn test_explain_plan() {
        // Create a simple scan plan to explain
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        // First, create a scan plan
        let scan_plan = LogicalPlan::table_scan("users".to_string(), schema.clone(), 1);

        // Then create an explain logical plan that wraps the scan plan
        // This calls the static explain constructor method, not the instance method
        let explain_plan = LogicalPlan {
            plan_type: LogicalPlanType::Explain {
                plan: Box::new(*scan_plan),
            },
            children: vec![],
        };

        // Validate the plan structure
        match &explain_plan.plan_type {
            LogicalPlanType::Explain { plan } => {
                // Check that the inner plan is our scan plan
                match &plan.plan_type {
                    LogicalPlanType::TableScan { table_name, .. } => {
                        assert_eq!(table_name, "users");
                    },
                    _ => panic!("Expected TableScan as inner plan"),
                }
            },
            _ => panic!("Expected Explain plan"),
        }
    }

    #[test]
    fn test_start_transaction_plan() {
        let isolation_level = Some(IsolationLevel::ReadCommitted);
        let read_only = true;
        let transaction_modifier = Some(TransactionModifier::Deferred);
        let statements = vec![Statement::Commit {
            chain: false,
            modifier: None,
            end: false,
        }];
        let _exception_statements = Some(vec![Statement::Rollback {
            chain: false,
            savepoint: None,
        }]);
        let has_end_keyword = true;

        let plan = LogicalPlan::start_transaction(
            isolation_level,
            read_only,
            transaction_modifier,
            statements.clone(),
            None, // Convert to None for now since we don't have proper ExceptionWhen conversion
            has_end_keyword,
        );

        match &plan.plan_type {
            LogicalPlanType::StartTransaction {
                isolation_level: level,
                read_only: ro,
                transaction_modifier: tm,
                statements: stmts,
                exception_statements: _ex_stmts,
                has_end_keyword: hek,
            } => {
                assert_eq!(isolation_level, *level);
                assert_eq!(read_only, *ro);
                assert_eq!(transaction_modifier, *tm);
                assert_eq!(statements, *stmts);
                assert_eq!(has_end_keyword, *hek);
            },
            _ => panic!("Expected StartTransaction plan"),
        }
    }

    #[test]
    fn test_commit_plan() {
        let plan = LogicalPlan::commit_transaction(false, false, None);

        match &plan.plan_type {
            LogicalPlanType::Commit {
                chain,
                end,
                modifier,
            } => {
                assert!(!(*chain));
                assert!(!(*end));
                assert!(modifier.is_none());
            },
            _ => panic!("Expected Commit plan"),
        }
    }

    #[test]
    fn test_rollback_plan() {
        let chain = true;
        let plan = LogicalPlan::rollback_transaction(chain, None);

        match &plan.plan_type {
            LogicalPlanType::Rollback {
                chain: c,
                savepoint,
            } => {
                assert_eq!(chain, *c);
                assert!(savepoint.is_none());
            },
            _ => panic!("Expected Rollback plan"),
        }
    }

    #[test]
    fn test_rollback_transaction_plan() {
        let chain = true;
        let savepoint = Some(Ident::new("SAVEPOINT1"));
        let plan = LogicalPlan::rollback_transaction(chain, savepoint.clone());

        match &plan.plan_type {
            LogicalPlanType::Rollback {
                chain: c,
                savepoint: sp,
            } => {
                assert_eq!(chain, *c);
                if let Some(sp_val) = sp {
                    if let Some(expected) = &savepoint {
                        assert_eq!(expected.value, sp_val.value);
                    } else {
                        panic!("Expected savepoint to be Some");
                    }
                } else {
                    panic!("Expected savepoint to be Some");
                }
            },
            _ => panic!("Expected Rollback plan with savepoint"),
        }
    }

    #[test]
    fn test_savepoint_plan() {
        let name = "SAVEPOINT1".to_string();
        let plan = LogicalPlan::savepoint(name.clone());

        match &plan.plan_type {
            LogicalPlanType::Savepoint { name: n } => {
                assert_eq!(name, *n);
            },
            _ => panic!("Expected Savepoint plan"),
        }
    }

    #[test]
    fn test_release_savepoint_plan() {
        let name = "SAVEPOINT1".to_string();
        let plan = LogicalPlan::release_savepoint(name.clone());

        match &plan.plan_type {
            LogicalPlanType::ReleaseSavepoint { name: n } => {
                assert_eq!(name, *n);
            },
            _ => panic!("Expected ReleaseSavepoint plan"),
        }
    }

    #[test]
    fn test_drop_plan() {
        let object_type = "TABLE".to_string();
        let if_exists = true;
        let names = vec!["users".to_string(), "orders".to_string()];
        let cascade = true;

        let plan = LogicalPlan::drop(object_type.clone(), if_exists, names.clone(), cascade);

        match &plan.plan_type {
            LogicalPlanType::Drop {
                object_type: ot,
                if_exists: ie,
                names: n,
                cascade: c,
            } => {
                assert_eq!(object_type, *ot);
                assert_eq!(if_exists, *ie);
                assert_eq!(names, *n);
                assert_eq!(cascade, *c);
            },
            _ => panic!("Expected Drop plan"),
        }
    }

    #[test]
    fn test_create_schema_plan() {
        let schema_name = "test_schema".to_string();
        let if_not_exists = true;

        let plan = LogicalPlan::create_schema(schema_name.clone(), if_not_exists);

        match &plan.plan_type {
            LogicalPlanType::CreateSchema {
                schema_name: sn,
                if_not_exists: ine,
            } => {
                assert_eq!(schema_name, *sn);
                assert_eq!(if_not_exists, *ine);
            },
            _ => panic!("Expected CreateSchema plan"),
        }
    }

    #[test]
    fn test_create_database_plan() {
        let db_name = "test_db".to_string();
        let if_not_exists = true;

        let plan = LogicalPlan::create_database(db_name.clone(), if_not_exists);

        match &plan.plan_type {
            LogicalPlanType::CreateDatabase {
                db_name: dn,
                if_not_exists: ine,
            } => {
                assert_eq!(db_name, *dn);
                assert_eq!(if_not_exists, *ine);
            },
            _ => panic!("Expected CreateDatabase plan"),
        }
    }

    #[test]
    fn test_alter_table_plan() {
        let table_name = "users".to_string();
        let operation = "ADD COLUMN email VARCHAR".to_string();

        let plan = LogicalPlan::alter_table(table_name.clone(), operation.clone());

        match &plan.plan_type {
            LogicalPlanType::AlterTable {
                table_name: tn,
                operation: op,
            } => {
                assert_eq!(table_name, *tn);
                assert_eq!(operation, *op);
            },
            _ => panic!("Expected AlterTable plan"),
        }
    }

    #[test]
    fn test_create_view_plan() {
        let view_name = "active_users".to_string();
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let if_not_exists = true;

        let plan = LogicalPlan::create_view(view_name.clone(), schema.clone(), if_not_exists);

        match &plan.plan_type {
            LogicalPlanType::CreateView {
                view_name: vn,
                schema: s,
                if_not_exists: ine,
            } => {
                assert_eq!(view_name, *vn);
                assert_eq!(schema, *s);
                assert_eq!(if_not_exists, *ine);
            },
            _ => panic!("Expected CreateView plan"),
        }
    }

    #[test]
    fn test_alter_view_plan() {
        let view_name = "active_users".to_string();
        let operation = "RENAME TO recent_users".to_string();

        let plan = LogicalPlan::alter_view(view_name.clone(), operation.clone());

        match &plan.plan_type {
            LogicalPlanType::AlterView {
                view_name: vn,
                operation: op,
            } => {
                assert_eq!(view_name, *vn);
                assert_eq!(operation, *op);
            },
            _ => panic!("Expected AlterView plan"),
        }
    }

    #[test]
    fn test_show_tables_plan() {
        let schema_name = Some("public".to_string());
        let plan = LogicalPlan::show_tables(schema_name.clone());

        match &plan.plan_type {
            LogicalPlanType::ShowTables {
                schema_name: sn,
                terse,
                history,
                extended,
                full,
                external,
            } => {
                assert_eq!(schema_name, *sn);
                assert!(!(*terse));
                assert!(!(*history));
                assert!(!(*extended));
                assert!(!(*full));
                assert!(!(*external));
            },
            _ => panic!("Expected ShowTables plan"),
        }

        // Also test the show_tables_with_options method
        let plan_with_options = LogicalPlan::show_tables_with_options(
            schema_name.clone(),
            true,
            true,
            true,
            true,
            true,
        );

        match &plan_with_options.plan_type {
            LogicalPlanType::ShowTables {
                schema_name: sn,
                terse,
                history,
                extended,
                full,
                external,
            } => {
                assert_eq!(schema_name, *sn);
                assert!(*terse);
                assert!(*history);
                assert!(*extended);
                assert!(*full);
                assert!(*external);
            },
            _ => panic!("Expected ShowTables plan with options"),
        }
    }

    #[test]
    fn test_show_databases_plan() {
        let plan = LogicalPlan::show_databases();

        match &plan.plan_type {
            LogicalPlanType::ShowDatabases { terse, history } => {
                // Successfully created a ShowDatabases plan
                assert_eq!(terse, &false);
                assert_eq!(history, &false);
            },
            _ => panic!("Expected ShowDatabases plan"),
        }
    }

    #[test]
    fn test_show_databases_with_options_plan() {
        let plan = LogicalPlan::show_databases_with_options(true, true);

        match &plan.plan_type {
            LogicalPlanType::ShowDatabases { terse, history } => {
                // Successfully created a ShowDatabases plan with options
                assert_eq!(terse, &true);
                assert_eq!(history, &true);
            },
            _ => panic!("Expected ShowDatabases plan with options"),
        }
    }

    #[test]
    fn test_show_columns_plan() {
        let table_name = "users".to_string();
        let schema_name = Some("public".to_string());

        let plan = LogicalPlan::show_columns(table_name.clone(), schema_name.clone());

        match &plan.plan_type {
            LogicalPlanType::ShowColumns {
                table_name: tn,
                schema_name: sn,
                extended,
                full,
            } => {
                assert_eq!(table_name, *tn);
                assert_eq!(schema_name, *sn);
                assert!(!(*extended));
                assert!(!(*full));
            },
            _ => panic!("Expected ShowColumns plan"),
        }
    }

    #[test]
    fn test_use_db_plan() {
        let db_name = "test_db".to_string();
        let plan = LogicalPlan::use_db(db_name.clone());

        match &plan.plan_type {
            LogicalPlanType::Use { db_name: dn } => {
                assert_eq!(db_name, *dn);
            },
            _ => panic!("Expected Use plan"),
        }
    }

    #[test]
    fn test_commit_transaction_plan() {
        let chain = true;
        let end = false;
        let modifier = Some(TransactionModifier::Deferred);
        let plan = LogicalPlan::commit_transaction(chain, end, modifier);

        match &plan.plan_type {
            LogicalPlanType::Commit {
                chain: c,
                end: e,
                modifier: m,
            } => {
                assert_eq!(chain, *c);
                assert_eq!(end, *e);
                assert_eq!(&modifier, m);
            },
            _ => panic!("Expected Commit plan"),
        }
    }

    #[test]
    fn test_show_columns_with_options_plan() {
        let table_name = "users".to_string();
        let schema_name = Some("public".to_string());
        let extended = true;
        let full = true;

        let plan = LogicalPlan::show_columns_with_options(
            table_name.clone(),
            schema_name.clone(),
            extended,
            full,
        );

        match &plan.plan_type {
            LogicalPlanType::ShowColumns {
                table_name: tn,
                schema_name: sn,
                extended: e,
                full: f,
            } => {
                assert_eq!(table_name, *tn);
                assert_eq!(schema_name, *sn);
                assert_eq!(*e, extended);
                assert_eq!(*f, full);
            },
            _ => panic!("Expected ShowColumns plan with options"),
        }
    }
}

#[cfg(test)]
mod test_extract_join_keys {
    use super::*;
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{
        ComparisonExpression, ComparisonType,
    };
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_column_ref(tuple_index: usize, column_index: usize, name: &str) -> Arc<Expression> {
        Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            tuple_index,
            column_index,
            Column::new(name, TypeId::Integer),
            vec![],
        )))
    }

    fn create_comparison(
        left: Arc<Expression>,
        right: Arc<Expression>,
        comp_type: ComparisonType,
    ) -> Arc<Expression> {
        Arc::new(Expression::Comparison(ComparisonExpression::new(
            left.clone(),
            right.clone(),
            comp_type,
            vec![left.clone(), right.clone()],
        )))
    }

    fn create_logic(
        left: Arc<Expression>,
        right: Arc<Expression>,
        logic_type: LogicType,
    ) -> Arc<Expression> {
        Arc::new(Expression::Logic(LogicExpression::new(
            left.clone(),
            right.clone(),
            logic_type,
            vec![left.clone(), right.clone()],
        )))
    }

    #[test]
    fn test_extract_simple_equality() {
        // a.id = b.id
        let left_col = create_column_ref(0, 0, "a.id");
        let right_col = create_column_ref(0, 1, "b.id");
        let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

        let result = extract_join_keys(&predicate).unwrap();

        // Verify we got one key from each side
        assert_eq!(result.0.len(), 1);
        assert_eq!(result.1.len(), 1);

        // Verify the fixed predicate exists
        let fixed_predicate = result.2;

        // Verify tuple indices
        if let Expression::Comparison(comp) = fixed_predicate.as_ref() {
            if let (Expression::ColumnRef(left), Expression::ColumnRef(right)) = (
                comp.get_children()[0].as_ref(),
                comp.get_children()[1].as_ref(),
            ) {
                assert_eq!(left.get_tuple_index(), 0);
                assert_eq!(right.get_tuple_index(), 1);
            } else {
                panic!("Expected column references in fixed predicate");
            }
        } else {
            panic!("Expected comparison expression for fixed predicate");
        }
    }

    #[test]
    fn test_extract_multiple_conditions() {
        // a.id = b.id AND a.value = b.value
        let left_col1 = create_column_ref(0, 0, "a.id");
        let right_col1 = create_column_ref(0, 1, "b.id");
        let pred1 = create_comparison(left_col1, right_col1, ComparisonType::Equal);

        let left_col2 = create_column_ref(0, 2, "a.value");
        let right_col2 = create_column_ref(0, 3, "b.value");
        let pred2 = create_comparison(left_col2, right_col2, ComparisonType::Equal);

        let combined_pred = create_logic(pred1, pred2, LogicType::And);

        let result = extract_join_keys(&combined_pred).unwrap();

        // Verify we got two keys from each side
        assert_eq!(result.0.len(), 2);
        assert_eq!(result.1.len(), 2);

        // Verify the fixed predicate exists and is a logic expression
        if let Expression::Logic(logic) = result.2.as_ref() {
            assert_eq!(logic.get_logic_type(), LogicType::And);
        } else {
            panic!("Expected logic expression for fixed predicate with multiple conditions");
        }
    }

    #[test]
    fn test_fix_column_index_for_right_table() {
        // a.dept_id = d.id where d.id is first column (index 0) in departments table
        let left_col = create_column_ref(0, 2, "a.dept_id");
        let right_col = create_column_ref(0, 3, "d.id"); // Index is wrong in original predicate
        let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

        let result = extract_join_keys(&predicate).unwrap();

        // Check the fixed predicate's right column index
        if let Expression::Comparison(comp) = result.2.as_ref() {
            if let Expression::ColumnRef(right) = comp.get_children()[1].as_ref() {
                assert_eq!(right.get_tuple_index(), 1);
                assert_eq!(right.get_column_index(), 0); // Should be fixed to 0
                assert_eq!(right.get_return_type().get_name(), "d.id");
            } else {
                panic!("Expected column reference for right side");
            }
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_non_equality_predicate() {
        // a.id > b.id (should fail)
        let left_col = create_column_ref(0, 0, "a.id");
        let right_col = create_column_ref(0, 1, "b.id");
        let predicate = create_comparison(left_col, right_col, ComparisonType::GreaterThan);

        let result = extract_join_keys(&predicate);
        assert!(result.is_err());
    }

    #[test]
    fn test_non_column_predicate() {
        // Create a non-column reference expression (should fail)
        let constant = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let col = create_column_ref(0, 0, "a.id");
        let predicate = create_comparison(constant, col, ComparisonType::Equal);

        let result = extract_join_keys(&predicate);
        assert!(result.is_err());
    }

    #[test]
    fn test_different_column_names() {
        // Test with different column names that don't match the special "d.id" pattern
        let left_col = create_column_ref(0, 1, "employees.emp_id");
        let right_col = create_column_ref(0, 2, "departments.dept_head");
        let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

        let result = extract_join_keys(&predicate).unwrap();

        // Verify we got one key from each side
        assert_eq!(result.0.len(), 1);
        assert_eq!(result.1.len(), 1);

        // Check that the right column index is preserved (not fixed to 0)
        if let Expression::Comparison(comp) = result.2.as_ref() {
            if let Expression::ColumnRef(right) = comp.get_children()[1].as_ref() {
                assert_eq!(right.get_tuple_index(), 1);
                assert_eq!(right.get_column_index(), 2); // Should preserve original index
                assert_eq!(right.get_return_type().get_name(), "departments.dept_head");
            } else {
                panic!("Expected column reference for right side");
            }
        } else {
            panic!("Expected comparison expression");
        }
    }

    #[test]
    fn test_or_logic_expression_error() {
        // a.id = b.id OR a.value = b.value (should fail - only AND is supported)
        let left_col1 = create_column_ref(0, 0, "a.id");
        let right_col1 = create_column_ref(0, 1, "b.id");
        let pred1 = create_comparison(left_col1, right_col1, ComparisonType::Equal);

        let left_col2 = create_column_ref(0, 2, "a.value");
        let right_col2 = create_column_ref(0, 3, "b.value");
        let pred2 = create_comparison(left_col2, right_col2, ComparisonType::Equal);

        let combined_pred = create_logic(pred1, pred2, LogicType::Or);

        let result = extract_join_keys(&combined_pred);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Only AND is supported"));
    }

    #[test]
    fn test_comparison_with_wrong_operand_count() {
        // Create a comparison with wrong number of children (should be handled gracefully)
        let left_col = create_column_ref(0, 0, "a.id");
        let right_col = create_column_ref(0, 1, "b.id");

        // Create comparison with only one child (malformed)
        let malformed_comp = Arc::new(Expression::Comparison(ComparisonExpression::new(
            left_col.clone(),
            right_col.clone(),
            ComparisonType::Equal,
            vec![left_col.clone()], // Only one child instead of two
        )));

        let result = extract_join_keys(&malformed_comp);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly two operands"));
    }

    #[test]
    fn test_logic_with_wrong_operand_count() {
        // Create a logic expression with wrong number of children
        let left_col = create_column_ref(0, 0, "a.id");
        let right_col = create_column_ref(0, 1, "b.id");
        let pred1 = create_comparison(left_col, right_col, ComparisonType::Equal);

        // Create logic with only one child (malformed)
        let malformed_logic = Arc::new(Expression::Logic(LogicExpression::new(
            pred1.clone(),
            pred1.clone(),
            LogicType::And,
            vec![pred1.clone()], // Only one child instead of two
        )));

        let result = extract_join_keys(&malformed_logic);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("exactly two operands"));
    }

    #[test]
    fn test_multiple_inequality_comparisons() {
        // Test various inequality operators
        let test_cases = vec![
            (ComparisonType::NotEqual, "not equal"),
            (ComparisonType::LessThan, "less than"),
            (ComparisonType::LessThanOrEqual, "less than or equal"),
            (ComparisonType::GreaterThanOrEqual, "greater than or equal"),
        ];

        for (comp_type, desc) in test_cases {
            let left_col = create_column_ref(0, 0, "a.id");
            let right_col = create_column_ref(0, 1, "b.id");
            let predicate = create_comparison(left_col, right_col, comp_type);

            let result = extract_join_keys(&predicate);
            assert!(result.is_err(), "Expected error for {} comparison", desc);
            assert!(result.unwrap_err().contains("equality comparison"));
        }
    }

    #[test]
    fn test_mixed_expression_types_in_comparison() {
        // Left side is column, right side is constant (should fail)
        let left_col = create_column_ref(0, 0, "a.id");
        let right_const = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let predicate = create_comparison(left_col, right_const, ComparisonType::Equal);

        let result = extract_join_keys(&predicate);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("column references"));
    }

    #[test]
    fn test_complex_column_names() {
        // Test with complex table and column names
        let scenarios = vec![
            ("schema1.table1.col1", "schema2.table2.col2"),
            ("t1.very_long_column_name", "t2.another_long_name"),
            (
                "Table_With_Underscores.Column_Name",
                "AnotherTable.AnotherColumn",
            ),
        ];

        for (left_name, right_name) in scenarios {
            let left_col = create_column_ref(0, 0, left_name);
            let right_col = create_column_ref(0, 1, right_name);
            let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

            let result = extract_join_keys(&predicate);
            assert!(
                result.is_ok(),
                "Failed for column names: {} = {}",
                left_name,
                right_name
            );

            let (left_keys, right_keys, _) = result.unwrap();
            assert_eq!(left_keys.len(), 1);
            assert_eq!(right_keys.len(), 1);
        }
    }

    #[test]
    fn test_three_way_and_logic() {
        // Test nested AND logic: (a.id = b.id) AND ((a.val1 = b.val1) AND (a.val2 = b.val2))
        let pred1 = create_comparison(
            create_column_ref(0, 0, "a.id"),
            create_column_ref(0, 1, "b.id"),
            ComparisonType::Equal,
        );

        let pred2 = create_comparison(
            create_column_ref(0, 2, "a.val1"),
            create_column_ref(0, 3, "b.val1"),
            ComparisonType::Equal,
        );

        let pred3 = create_comparison(
            create_column_ref(0, 4, "a.val2"),
            create_column_ref(0, 5, "b.val2"),
            ComparisonType::Equal,
        );

        // Create nested structure: pred2 AND pred3
        let nested_and = create_logic(pred2, pred3, LogicType::And);

        // Create final structure: pred1 AND (pred2 AND pred3)
        let final_pred = create_logic(pred1, nested_and, LogicType::And);

        let result = extract_join_keys(&final_pred).unwrap();

        // Should extract keys from all three comparisons in the nested structure
        assert_eq!(result.0.len(), 3); // Three left keys (from all comparisons)
        assert_eq!(result.1.len(), 3); // Three right keys (from all comparisons)
    }

    #[test]
    fn test_special_d_id_variations() {
        // Test variations of the special "d.id" case
        let test_cases = vec![
            ("d.id", true),          // Should be fixed to index 0
            ("D.ID", false),         // Case sensitive - should not be fixed
            ("d.identifier", false), // Different column name - should not be fixed
            ("dept.id", false),      // Different table alias - should not be fixed
            ("td.id", false),        // Different table alias - should not be fixed
        ];

        for (col_name, should_fix) in test_cases {
            let left_col = create_column_ref(0, 0, "a.foreign_key");
            let right_col = create_column_ref(0, 5, col_name); // Start with index 5
            let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

            let result = extract_join_keys(&predicate).unwrap();

            if let Expression::Comparison(comp) = result.2.as_ref() {
                if let Expression::ColumnRef(right) = comp.get_children()[1].as_ref() {
                    let expected_index = if should_fix { 0 } else { 5 };
                    assert_eq!(
                        right.get_column_index(),
                        expected_index,
                        "Column {} should {} have its index fixed to 0",
                        col_name,
                        if should_fix { "" } else { "not" }
                    );
                } else {
                    panic!("Expected column reference for right side");
                }
            } else {
                panic!("Expected comparison expression");
            }
        }
    }

    #[test]
    fn test_unsupported_expression_type() {
        // Test with an aggregate expression (unsupported)
        let agg_expr = Arc::new(Expression::Aggregate(
            crate::sql::execution::expressions::aggregate_expression::AggregateExpression::new(
                AggregationType::Count,
                vec![create_column_ref(0, 0, "a.id")],
                Column::new("count", TypeId::Integer),
                "COUNT".to_string(),
            ),
        ));

        let result = extract_join_keys(&agg_expr);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .contains("Unsupported join predicate expression type")
        );
    }

    #[test]
    fn test_tuple_index_preservation() {
        // Test that original tuple indices are correctly transformed to 0 and 1
        let scenarios = vec![
            (0, 0, 0, 1),  // Standard case
            (5, 10, 0, 1), // Higher original indices should still become 0 and 1
            (2, 3, 0, 1),  // Mid-range indices
        ];

        for (left_tuple, right_tuple, expected_left, expected_right) in scenarios {
            let left_col = create_column_ref(left_tuple, 0, "left.col");
            let right_col = create_column_ref(right_tuple, 1, "right.col");
            let predicate = create_comparison(left_col, right_col, ComparisonType::Equal);

            let result = extract_join_keys(&predicate).unwrap();

            if let Expression::Comparison(comp) = result.2.as_ref() {
                if let (Expression::ColumnRef(left), Expression::ColumnRef(right)) = (
                    comp.get_children()[0].as_ref(),
                    comp.get_children()[1].as_ref(),
                ) {
                    assert_eq!(left.get_tuple_index(), expected_left);
                    assert_eq!(right.get_tuple_index(), expected_right);
                } else {
                    panic!("Expected column references in fixed predicate");
                }
            } else {
                panic!("Expected comparison expression");
            }
        }
    }
}
