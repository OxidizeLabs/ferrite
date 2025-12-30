//! # Query Optimizer
//!
//! This module implements a **rule-based query optimizer** that transforms logical plans
//! into more efficient forms before physical plan generation. The optimizer applies a
//! sequence of optimization phases to reduce query execution cost.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────────────────────┐
//! │                           Optimization Pipeline                                 │
//! ├─────────────────────────────────────────────────────────────────────────────────┤
//! │                                                                                 │
//! │   ┌─────────────────────┐                                                       │
//! │   │   Input: LogicalPlan│                                                       │
//! │   │   (from planner)    │                                                       │
//! │   └──────────┬──────────┘                                                       │
//! │              │                                                                  │
//! │              ▼                                                                  │
//! │   ┌──────────────────────────────────────────────────────────────────────┐      │
//! │   │  Phase 0: Constraint Index Creation                                  │      │
//! │   │  - Create PRIMARY KEY indexes                                        │      │
//! │   │  - Create UNIQUE constraint indexes                                  │      │
//! │   │  - Create FOREIGN KEY indexes                                        │      │
//! │   │  - Create referenced column indexes                                  │      │
//! │   └──────────────────────────────────────────────────────────────────────┘      │
//! │              │                                                                  │
//! │              ▼                                                                  │
//! │   ┌──────────────────────────────────────────────────────────────────────┐      │
//! │   │  Phase 1: Index Scan Optimization + Early Pruning                    │      │
//! │   │  - Convert TableScan → IndexScan when beneficial                     │      │
//! │   │  - Remove empty projections                                          │      │
//! │   │  - Eliminate always-true filters                                     │      │
//! │   └──────────────────────────────────────────────────────────────────────┘      │
//! │              │                                                                  │
//! │              ▼                                                                  │
//! │   ┌──────────────────────────────────────────────────────────────────────┐      │
//! │   │  Phase 2: Rewrite Rules                                              │      │
//! │   │  - Predicate pushdown (push filters closer to data sources)          │      │
//! │   │  - Projection merging (combine consecutive projections)              │      │
//! │   └──────────────────────────────────────────────────────────────────────┘      │
//! │              │                                                                  │
//! │              ▼                                                                  │
//! │   ┌──────────────────────────────────────────────────────────────────────┐      │
//! │   │  Phase 3: Join Optimization (if EnableNljCheck)                      │      │
//! │   │  - NestedLoopJoin → HashJoin for equi-joins                          │      │
//! │   │  - Join reordering (TODO)                                            │      │
//! │   └──────────────────────────────────────────────────────────────────────┘      │
//! │              │                                                                  │
//! │              ▼                                                                  │
//! │   ┌──────────────────────────────────────────────────────────────────────┐      │
//! │   │  Phase 4: Sort & Limit Optimization (if EnableTopnCheck)             │      │
//! │   │  - Sort + Limit → TopN (heap-based selection)                        │      │
//! │   └──────────────────────────────────────────────────────────────────────┘      │
//! │              │                                                                  │
//! │              ▼                                                                  │
//! │   ┌──────────────────────────────────────────────────────────────────────┐      │
//! │   │  Validation                                                          │      │
//! │   │  - Verify table existence                                            │      │
//! │   │  - Check join predicates                                             │      │
//! │   │  - Validate TopN specifications                                      │      │
//! │   └──────────────────────────────────────────────────────────────────────┘      │
//! │              │                                                                  │
//! │              ▼                                                                  │
//! │   ┌─────────────────────┐                                                       │
//! │   │  Output: LogicalPlan│                                                       │
//! │   │  (optimized)        │                                                       │
//! │   └─────────────────────┘                                                       │
//! │                                                                                 │
//! └─────────────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Optimization Phases
//!
//! | Phase | Name | Description | Condition |
//! |-------|------|-------------|-----------|
//! | 0 | Constraint Indexes | Auto-create indexes for PK/UNIQUE/FK constraints | Always |
//! | 1 | Index Scan + Pruning | Replace table scans with index scans; remove no-op nodes | Always |
//! | 2 | Rewrite Rules | Predicate pushdown, projection merging | Always |
//! | 3 | Join Optimization | Convert NLJ to HashJoin for equi-joins | `EnableNljCheck` |
//! | 4 | Sort+Limit → TopN | Use heap-based TopN instead of full sort | `EnableTopnCheck` |
//!
//! ## Constraint Index Creation (Phase 0)
//!
//! Automatically creates B+ tree indexes for constraint validation, converting O(n)
//! constraint checks to O(log n) index lookups:
//!
//! ```text
//! CREATE TABLE orders (
//!     id INT PRIMARY KEY,        ─────▶  orders_pk_idx (B+ tree, UNIQUE)
//!     email VARCHAR UNIQUE,      ─────▶  orders_email_unique_idx (B+ tree, UNIQUE)
//!     customer_id INT REFERENCES ─────▶  orders_customer_id_fk_idx (B+ tree)
//!         customers(id)
//! );
//! ```
//!
//! ## Predicate Pushdown (Phase 2)
//!
//! Moves filter predicates as close to data sources as possible:
//!
//! ```text
//! Before:                          After:
//!
//!     Filter(age > 21)                 Projection
//!          │                                │
//!     Projection                       Filter(age > 21)
//!          │                                │
//!     TableScan(users)                 TableScan(users)
//! ```
//!
//! ## TopN Optimization (Phase 4)
//!
//! Replaces Sort + Limit with a more efficient heap-based TopN:
//!
//! ```text
//! Before:                          After:
//!
//!     Limit(10)                        TopN(k=10, ORDER BY score DESC)
//!         │                                     │
//!     Sort(score DESC)                     TableScan
//!         │
//!     TableScan
//!
//! Complexity: O(n log n) → O(n log k)
//! Memory:     O(n)       → O(k)
//! ```
//!
//! ## Check Options
//!
//! Optimization behavior is controlled by [`CheckOptions`]:
//!
//! | Option | Effect |
//! |--------|--------|
//! | `is_modify()` | Master switch - if false, skip all optimizations |
//! | `EnableNljCheck` | Enable join optimization (NLJ → HashJoin) |
//! | `EnableTopnCheck` | Enable Sort+Limit → TopN optimization |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use tkdb::sql::optimizer::optimizer_impl::Optimizer;
//! use tkdb::sql::execution::check_option::CheckOptions;
//!
//! let catalog = Arc::new(RwLock::new(Catalog::new()));
//! let optimizer = Optimizer::new(catalog);
//!
//! let check_options = Arc::new(CheckOptions::default());
//! let optimized_plan = optimizer.optimize(logical_plan, check_options)?;
//! ```
//!
//! ## Validation
//!
//! After optimization, the plan is validated:
//! - Tables referenced by `TableScan` must exist in the catalog
//! - `NestedLoopJoin` must have a predicate (if NLJ check enabled)
//! - `TopN` must have valid k and sort specifications
//!
//! ## Thread Safety
//!
//! - Uses `Arc<RwLock<Catalog>>` for safe concurrent catalog access
//! - Read lock for index lookups, write lock for index creation
//! - Optimization itself is single-threaded per query

use crate::catalog::Catalog;
use crate::catalog::schema::Schema;
use crate::common::exception::DBError;
use crate::sql::execution::check_option::{CheckOption, CheckOptions};
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::logic_expression::{LogicExpression, LogicType};
use crate::sql::planner::logical_plan::{LogicalPlan, LogicalPlanType};
use crate::types_db::value::Val;
use log::{debug, info, trace, warn};
use parking_lot::RwLock;
use std::sync::Arc;

// Additional imports for constraint index creation
use crate::storage::index::IndexType;

/// Rule-based query optimizer that transforms logical plans into more efficient forms.
///
/// The optimizer applies a sequence of optimization phases to reduce query execution
/// cost before physical plan generation. Each phase targets specific optimization
/// opportunities.
///
/// # Optimization Phases
///
/// | Phase | Name | Description |
/// |-------|------|-------------|
/// | 0 | Constraint Indexes | Auto-create indexes for PK/UNIQUE/FK |
/// | 1 | Index Scan + Pruning | Replace table scans; remove no-op nodes |
/// | 2 | Rewrite Rules | Predicate pushdown, projection merging |
/// | 3 | Join Optimization | Convert NLJ to HashJoin (if enabled) |
/// | 4 | Sort+Limit → TopN | Use heap-based TopN (if enabled) |
///
/// # Thread Safety
///
/// Uses `Arc<RwLock<Catalog>>` for safe concurrent catalog access.
/// Read lock for lookups, write lock for index creation.
///
/// # Example
///
/// ```rust,ignore
/// let optimizer = Optimizer::new(catalog.clone());
/// let check_options = Arc::new(CheckOptions::default());
/// let optimized_plan = optimizer.optimize(logical_plan, check_options)?;
/// ```
pub struct Optimizer {
    /// Shared catalog for schema lookups and index creation.
    catalog: Arc<RwLock<Catalog>>,
}

impl Optimizer {
    /// Creates a new optimizer with access to the given catalog.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Shared catalog for schema and index information
    pub fn new(catalog: Arc<RwLock<Catalog>>) -> Self {
        Self { catalog }
    }

    /// Main entry point for query optimization.
    ///
    /// Applies all enabled optimization phases to the logical plan, then
    /// validates the result before returning.
    ///
    /// # Arguments
    ///
    /// * `logical_plan` - The unoptimized logical plan from the planner
    /// * `check_options` - Flags controlling which optimizations to apply
    ///
    /// # Returns
    ///
    /// * `Ok(Box<LogicalPlan>)` - The optimized logical plan
    /// * `Err(DBError)` - If optimization or validation fails
    ///
    /// # Phases Applied
    ///
    /// 1. **Constraint Indexes** (always): Create indexes for constraints
    /// 2. **Index Scan** (always): Convert table scans to index scans
    /// 3. **Early Pruning** (always): Remove empty projections, true filters
    /// 4. **Rewrite Rules** (always): Predicate pushdown, projection merge
    /// 5. **Join Optimization** (if `EnableNljCheck`): NLJ → HashJoin
    /// 6. **TopN Optimization** (if `EnableTopnCheck`): Sort+Limit → TopN
    pub fn optimize(
        &self,
        logical_plan: Box<LogicalPlan>,
        check_options: Arc<CheckOptions>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        info!("Starting query optimization");
        debug!("Initial plan:\n{:?}", logical_plan);

        let mut plan = logical_plan;

        if check_options.is_modify() {
            debug!("Modifications enabled, applying optimization phases");

            // Phase 0: Constraint index creation (PERFORMANCE OPTIMIZATION)
            trace!("Phase 0: Creating constraint indexes");
            self.create_constraint_indexes(&mut plan)?;

            // Phase 1: Index scan conversion and early pruning
            trace!("Phase 1: Starting index scan optimization");
            plan = self.apply_index_scan_optimization(plan)?;
            trace!("Phase 1: Starting early pruning");
            plan = self.apply_early_pruning(plan)?;

            // Phase 2: Rewrite rules
            trace!("Phase 2: Starting rewrite rules application");
            plan = self.apply_rewrite_rules(plan)?;

            // Phase 3: Join optimization
            if check_options.has_check(&CheckOption::EnableNljCheck) {
                trace!("Phase 3: Starting join optimization");
                plan = self.optimize_joins(plan)?;
            } else {
                debug!("Skipping join optimization - NLJ check not enabled");
            }

            // Phase 4: Sort and limit optimization
            if check_options.has_check(&CheckOption::EnableTopnCheck) {
                trace!("Phase 4: Starting sort and limit optimization");
                plan = self.optimize_sort_and_limit(plan)?;
            } else {
                debug!("Skipping sort optimization - TopN check not enabled");
            }
        } else {
            info!("Skipping optimization - modifications not enabled");
        }

        // Final validation
        trace!("Performing final plan validation");
        self.validate_plan(&plan, &check_options)?;

        debug!("Final optimized plan:\n{:?}", plan);
        info!("Query optimization completed successfully");

        Ok(plan)
    }

    /// Phase 0: Automatically creates indexes for constraint validation.
    ///
    /// Replaces O(n) table scans with O(log n) B+ tree index lookups for
    /// constraint checking during INSERT/UPDATE operations.
    ///
    /// # Indexes Created
    ///
    /// | Constraint Type | Index Name Format | Unique? |
    /// |-----------------|-------------------|---------|
    /// | PRIMARY KEY | `{table}_pk_idx` | Yes |
    /// | UNIQUE | `{table}_{col}_unique_idx` | Yes |
    /// | FOREIGN KEY | `{table}_{col}_fk_idx` | No |
    /// | Referenced Column | `{table}_{col}_ref_idx` | No |
    ///
    /// # Arguments
    ///
    /// * `plan` - The logical plan to analyze for constraint opportunities
    ///
    /// # Behavior
    ///
    /// - For `CreateTable`: Creates indexes for all constraints in the schema
    /// - For `TableScan`: Creates missing constraint indexes for existing tables
    /// - Recursively processes child nodes
    fn create_constraint_indexes(&self, plan: &mut Box<LogicalPlan>) -> Result<(), DBError> {
        trace!("Analyzing plan for constraint index creation opportunities");

        match &plan.plan_type {
            LogicalPlanType::CreateTable {
                table_name, schema, ..
            } => {
                info!("Creating constraint indexes for new table '{}'", table_name);
                self.create_table_constraint_indexes(table_name, schema)?;
            },
            LogicalPlanType::TableScan { table_name, .. } => {
                // Check if this table needs constraint indexes
                let catalog = self.catalog.read();
                if let Some(table_info) = catalog.get_table(table_name) {
                    let schema = table_info.get_table_schema();

                    // Check if constraint indexes are missing
                    if self.needs_constraint_indexes(table_name, &schema) {
                        drop(catalog); // Release read lock
                        info!(
                            "Creating missing constraint indexes for table '{}'",
                            table_name
                        );
                        self.create_table_constraint_indexes(table_name, &schema)?;
                    }
                }
            },
            _ => {},
        }

        // Recursively process children
        for child in &mut plan.children {
            self.create_constraint_indexes(child)?;
        }

        Ok(())
    }

    /// Creates all constraint indexes for a specific table.
    ///
    /// Iterates through the schema to find constraints and creates appropriate
    /// B+ tree indexes. Logs success/failure for each index creation.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to create indexes for
    /// * `schema` - Table schema containing column definitions and constraints
    fn create_table_constraint_indexes(
        &self,
        table_name: &str,
        schema: &Schema,
    ) -> Result<(), DBError> {
        let mut catalog = self.catalog.write();

        // 1. Create PRIMARY KEY index (composite or single column)
        let primary_key_columns: Vec<usize> = schema
            .get_columns()
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_primary_key())
            .map(|(i, _)| i)
            .collect();

        if !primary_key_columns.is_empty() {
            let pk_index_name = format!("{}_pk_idx", table_name);
            let pk_key_schema = Schema::copy_schema(schema, &primary_key_columns);
            let pk_key_size = pk_key_schema.get_inlined_storage_size() as usize;

            match catalog.create_index(
                &pk_index_name,
                table_name,
                pk_key_schema,
                primary_key_columns.clone(),
                pk_key_size,
                true, // PRIMARY KEY implies UNIQUE
                IndexType::BPlusTreeIndex,
            ) {
                Some(_) => {
                    info!(
                        "✓ Created PRIMARY KEY index '{}' for table '{}'",
                        pk_index_name, table_name
                    );
                },
                None => {
                    warn!(
                        "✗ Failed to create PRIMARY KEY index for table '{}'",
                        table_name
                    );
                },
            }
        }

        // 2. Create UNIQUE indexes for individual UNIQUE constraints (not part of PRIMARY KEY)
        for (col_idx, column) in schema.get_columns().iter().enumerate() {
            if column.is_unique() && !column.is_primary_key() {
                let unique_index_name = format!("{}_{}_unique_idx", table_name, column.get_name());
                let unique_key_schema = Schema::copy_schema(schema, &[col_idx]);
                let unique_key_size = column.get_storage_size();

                match catalog.create_index(
                    &unique_index_name,
                    table_name,
                    unique_key_schema,
                    vec![col_idx],
                    unique_key_size,
                    true, // UNIQUE constraint
                    IndexType::BPlusTreeIndex,
                ) {
                    Some(_) => {
                        info!(
                            "✓ Created UNIQUE index '{}' for column '{}' in table '{}'",
                            unique_index_name,
                            column.get_name(),
                            table_name
                        );
                    },
                    None => {
                        warn!(
                            "✗ Failed to create UNIQUE index for column '{}' in table '{}'",
                            column.get_name(),
                            table_name
                        );
                    },
                }
            }
        }

        // 3. Create FOREIGN KEY indexes for referencing columns
        for (col_idx, column) in schema.get_columns().iter().enumerate() {
            if let Some(fk_constraint) = column.get_foreign_key() {
                let fk_index_name = format!("{}_{}_fk_idx", table_name, column.get_name());
                let fk_key_schema = Schema::copy_schema(schema, &[col_idx]);
                let fk_key_size = column.get_storage_size();

                match catalog.create_index(
                    &fk_index_name,
                    table_name,
                    fk_key_schema,
                    vec![col_idx],
                    fk_key_size,
                    false, // FOREIGN KEY doesn't imply UNIQUE
                    IndexType::BPlusTreeIndex,
                ) {
                    Some(_) => {
                        info!(
                            "✓ Created FOREIGN KEY index '{}' for column '{}' referencing '{}.{}'",
                            fk_index_name,
                            column.get_name(),
                            fk_constraint.referenced_table,
                            fk_constraint.referenced_column
                        );
                    },
                    None => {
                        warn!(
                            "✗ Failed to create FOREIGN KEY index for column '{}' in table '{}'",
                            column.get_name(),
                            table_name
                        );
                    },
                }

                // PERFORMANCE OPTIMIZATION: Also create index on referenced column if it doesn't already exist
                self.ensure_referenced_column_index(
                    &mut catalog,
                    &fk_constraint.referenced_table,
                    &fk_constraint.referenced_column,
                )
                .map_err(|e| {
                    warn!("✗ Failed to create index on referenced column: {}", e);
                })
                .ok();
            }
        }

        Ok(())
    }

    /// Determines if a table is missing any constraint indexes.
    ///
    /// Checks for the existence of PRIMARY KEY, UNIQUE, and FOREIGN KEY
    /// indexes using the standard naming convention.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to check
    /// * `schema` - Table schema with constraint definitions
    ///
    /// # Returns
    ///
    /// `true` if any expected constraint index is missing.
    fn needs_constraint_indexes(&self, table_name: &str, schema: &Schema) -> bool {
        let catalog = self.catalog.read();
        let existing_indexes = catalog.get_table_indexes(table_name);

        // Check if PRIMARY KEY index exists
        let primary_key_columns: Vec<usize> = schema
            .get_columns()
            .iter()
            .enumerate()
            .filter(|(_, col)| col.is_primary_key())
            .map(|(i, _)| i)
            .collect();

        if !primary_key_columns.is_empty() {
            let pk_index_name = format!("{}_pk_idx", table_name);
            let has_pk_index = existing_indexes
                .iter()
                .any(|idx| idx.get_index_name() == &pk_index_name);
            if !has_pk_index {
                return true;
            }
        }

        // Check if UNIQUE indexes exist
        for column in schema.get_columns().iter() {
            if column.is_unique() && !column.is_primary_key() {
                let unique_index_name = format!("{}_{}_unique_idx", table_name, column.get_name());
                let has_unique_index = existing_indexes
                    .iter()
                    .any(|idx| idx.get_index_name() == &unique_index_name);
                if !has_unique_index {
                    return true;
                }
            }
        }

        // Check if FOREIGN KEY indexes exist
        for column in schema.get_columns().iter() {
            if column.get_foreign_key().is_some() {
                let fk_index_name = format!("{}_{}_fk_idx", table_name, column.get_name());
                let has_fk_index = existing_indexes
                    .iter()
                    .any(|idx| idx.get_index_name() == &fk_index_name);
                if !has_fk_index {
                    return true;
                }
            }
        }

        false
    }

    /// Creates an index on a referenced column for foreign key validation.
    ///
    /// When a FOREIGN KEY references another table's column, this method
    /// ensures the referenced column has an index for efficient lookup
    /// during constraint validation.
    ///
    /// # Arguments
    ///
    /// * `catalog` - Write-locked catalog for index creation
    /// * `referenced_table` - Name of the table being referenced
    /// * `referenced_column` - Name of the column being referenced
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Index created or already exists
    /// * `Err` - Referenced table doesn't exist
    fn ensure_referenced_column_index(
        &self,
        catalog: &mut parking_lot::RwLockWriteGuard<Catalog>,
        referenced_table: &str,
        referenced_column: &str,
    ) -> Result<(), String> {
        if let Some(table_info) = catalog.get_table(referenced_table) {
            let referenced_table_schema = table_info.get_table_schema();
            if let Some(column_index) = referenced_table_schema.get_column_index(referenced_column)
            {
                let column = referenced_table_schema.get_column(column_index).unwrap();
                let index_name = format!("{}_{}_ref_idx", referenced_table, column.get_name());

                // Check if index already exists
                let existing_indexes = catalog.get_table_indexes(referenced_table);
                let index_exists = existing_indexes
                    .iter()
                    .any(|idx| idx.get_index_name() == &index_name);

                if !index_exists {
                    let index_key_schema =
                        Schema::copy_schema(&referenced_table_schema, &[column_index]);
                    let index_key_size = column.get_storage_size();

                    match catalog.create_index(
                        &index_name,
                        referenced_table,
                        index_key_schema,
                        vec![column_index],
                        index_key_size,
                        false, // Referenced column doesn't need to be unique
                        IndexType::BPlusTreeIndex,
                    ) {
                        Some(_) => {
                            info!(
                                "✓ Created referenced column index '{}' for '{}.{}'",
                                index_name, referenced_table, referenced_column
                            );
                        },
                        None => {
                            warn!(
                                "✗ Failed to create referenced column index for '{}.{}'",
                                referenced_table, referenced_column
                            );
                        },
                    }
                }
            }
        } else {
            return Err(format!(
                "Referenced table '{}' does not exist",
                referenced_table
            ));
        }

        Ok(())
    }

    /// Phase 1: Converts table scans to index scans where beneficial.
    ///
    /// Examines each `TableScan` node and checks if an index exists on the
    /// first projected column. If so, converts to an `IndexScan` for more
    /// efficient data access.
    ///
    /// # Arguments
    ///
    /// * `plan` - The logical plan to optimize
    ///
    /// # Returns
    ///
    /// The plan with table scans converted to index scans where applicable.
    fn apply_index_scan_optimization(
        &self,
        plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        trace!(
            "Attempting index scan optimization for plan node: {:?}",
            plan.plan_type
        );

        match &plan.plan_type {
            LogicalPlanType::TableScan {
                table_name,
                schema,
                table_oid,
            } => {
                debug!(
                    "Examining table scan on {} for potential index usage",
                    table_name
                );

                let catalog = self.catalog.read();
                let indexes = catalog.get_table_indexes(table_name);

                if indexes.is_empty() {
                    debug!("No indexes found for table {}", table_name);
                    return Ok(plan);
                }

                if let Some(first_col) = schema.get_columns().first() {
                    trace!("Checking indexes for column {}", first_col.get_name());

                    for index_info in indexes {
                        let key_attrs = index_info.get_key_attrs();

                        if !key_attrs.is_empty()
                            && let Some(table_info) = catalog.get_table(table_name)
                        {
                            let table_schema = table_info.get_table_schema();
                            if let Some(col_idx) =
                                table_schema.get_column_index(first_col.get_name())
                                && key_attrs[0] == col_idx
                            {
                                info!(
                                    "Converting table scan to index scan using index {} on table {}",
                                    index_info.get_index_name(),
                                    table_name
                                );

                                return Ok(Box::new(LogicalPlan::new(
                                    LogicalPlanType::IndexScan {
                                        table_name: table_name.clone(),
                                        table_oid: *table_oid,
                                        index_name: index_info.get_index_name().to_string(),
                                        index_oid: index_info.get_index_oid(),
                                        schema: schema.clone(),
                                        predicate_keys: vec![],
                                    },
                                    vec![],
                                )));
                            }
                        }
                    }
                    debug!("No suitable index found for the first column");
                }
                Ok(plan)
            },
            _ => {
                trace!("Non-table-scan node, checking children");
                self.apply_index_scan_to_children(plan)
            },
        }
    }

    /// Recursively applies index scan optimization to child nodes.
    fn apply_index_scan_to_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        // Recursively try to convert any table scans in child nodes
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.apply_index_scan_optimization(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    /// Phase 1: Removes unnecessary nodes from the plan tree.
    ///
    /// Applies the following simplifications:
    /// - **Empty Projection**: Removes projections with no expressions
    /// - **True Filter**: Removes filters with constant `true` predicates
    ///
    /// # Arguments
    ///
    /// * `plan` - The logical plan to simplify
    ///
    /// # Returns
    ///
    /// The simplified plan with unnecessary nodes removed.
    fn apply_early_pruning(&self, mut plan: Box<LogicalPlan>) -> Result<Box<LogicalPlan>, DBError> {
        trace!("Applying early pruning to plan node: {:?}", plan.plan_type);

        match &plan.plan_type {
            LogicalPlanType::Projection { expressions, .. } => {
                if expressions.is_empty() {
                    debug!("Found empty projection, removing unnecessary node");
                    if let Some(child) = plan.children.pop() {
                        return Ok(child);
                    }
                }
                self.apply_early_pruning_to_children(plan)
            },
            LogicalPlanType::Filter {
                schema: _,
                output_schema: _,
                table_oid: _,
                table_name: _,
                predicate,
            } => {
                trace!("Examining filter predicate for simplification");
                if let Expression::Constant(const_expr) = predicate.as_ref()
                    && let Val::Boolean(b) = const_expr.get_value().value_
                    && b
                {
                    debug!("Removing true filter predicate");
                    if let Some(child) = plan.children.pop() {
                        return Ok(child);
                    }
                }
                self.apply_early_pruning_to_children(plan)
            },
            _ => self.apply_early_pruning_to_children(plan),
        }
    }

    /// Phase 2: Applies algebraic rewrite rules to optimize the plan.
    ///
    /// Implements the following transformations:
    /// - **Predicate Pushdown**: Moves filter predicates closer to data sources
    /// - **Projection Merging**: Combines consecutive projection nodes
    ///
    /// # Arguments
    ///
    /// * `plan` - The logical plan to rewrite
    ///
    /// # Returns
    ///
    /// The rewritten plan with optimizations applied.
    fn apply_rewrite_rules(&self, mut plan: Box<LogicalPlan>) -> Result<Box<LogicalPlan>, DBError> {
        match &plan.plan_type {
            LogicalPlanType::Filter {
                schema: _,
                output_schema: _,
                table_oid: _,
                table_name: _,
                predicate,
            } => {
                // Push down filter predicates
                if let Some(child) = plan.children.pop() {
                    self.push_down_predicate(predicate.clone(), child)
                } else {
                    Ok(plan)
                }
            },
            LogicalPlanType::Projection {
                expressions,
                schema: _,
                column_mappings: _,
            } => {
                // Combine consecutive projections
                if let Some(child) = plan.children.pop() {
                    if let LogicalPlanType::Projection { .. } = child.plan_type {
                        return self.merge_projections(expressions.clone(), child);
                    }
                    plan.children = vec![self.apply_rewrite_rules(child)?];
                    Ok(plan)
                } else {
                    Ok(plan)
                }
            },
            _ => self.apply_rewrite_rules_to_children(plan),
        }
    }

    /// Phase 3: Optimizes join operations for better performance.
    ///
    /// Converts nested loop joins to hash joins when beneficial:
    /// - Equi-joins with equality predicates
    /// - Large input relations that justify hash table overhead
    ///
    /// # Arguments
    ///
    /// * `plan` - The logical plan containing join nodes
    ///
    /// # Returns
    ///
    /// The plan with optimized join operators.
    ///
    /// # Note
    ///
    /// Only applied when `CheckOption::EnableNljCheck` is set.
    fn optimize_joins(&self, mut plan: Box<LogicalPlan>) -> Result<Box<LogicalPlan>, DBError> {
        match &plan.plan_type {
            LogicalPlanType::NestedLoopJoin {
                left_schema,
                right_schema,
                predicate,
                join_type,
            } => {
                // Take ownership of children
                let mut children = plan.children.drain(..).collect::<Vec<_>>();
                let right = children.pop().unwrap();
                let left = children.pop().unwrap();

                // Recursively optimize children first
                let optimized_left = self.optimize_joins(left)?;
                let optimized_right = self.optimize_joins(right)?;

                // Try to convert to hash join if beneficial
                if self.should_use_hash_join(predicate.as_ref()) {
                    Ok(Box::new(LogicalPlan::new(
                        LogicalPlanType::HashJoin {
                            left_schema: left_schema.clone(),
                            right_schema: right_schema.clone(),
                            predicate: predicate.clone(),
                            join_type: join_type.clone(),
                        },
                        vec![optimized_left, optimized_right],
                    )))
                } else {
                    plan.children = vec![optimized_left, optimized_right];
                    Ok(plan)
                }
            },
            _ => self.optimize_joins_children(plan),
        }
    }

    /// Phase 4: Optimizes Sort + Limit patterns into TopN operators.
    ///
    /// Replaces a full sort followed by a limit with a heap-based TopN:
    ///
    /// ```text
    /// Before: Limit(10) → Sort(score DESC) → Scan
    /// After:  TopN(k=10, score DESC) → Scan
    ///
    /// Complexity: O(n log n) → O(n log k)
    /// Memory:     O(n)       → O(k)
    /// ```
    ///
    /// # Arguments
    ///
    /// * `plan` - The logical plan to optimize
    ///
    /// # Returns
    ///
    /// The plan with Sort+Limit converted to TopN where applicable.
    ///
    /// # Note
    ///
    /// Only applied when `CheckOption::EnableTopnCheck` is set.
    fn optimize_sort_and_limit(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        match &plan.plan_type {
            LogicalPlanType::Sort {
                sort_specifications,
                schema,
            } => {
                if let Some(child) = plan.children.pop() {
                    // Look for opportunities to use TopN instead of Sort
                    if let LogicalPlanType::Limit { limit, .. } = child.plan_type {
                        return Ok(Box::new(LogicalPlan::new(
                            LogicalPlanType::TopN {
                                k: limit,
                                sort_specifications: sort_specifications.clone(),
                                schema: schema.clone(),
                            },
                            vec![self.optimize_sort_and_limit(child)?],
                        )));
                    }
                    plan.children = vec![self.optimize_sort_and_limit(child)?];
                }
                Ok(plan)
            },
            _ => self.optimize_sort_and_limit_children(plan),
        }
    }

    /// Recursively applies early pruning to child nodes.
    fn apply_early_pruning_to_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.apply_early_pruning(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    /// Recursively applies rewrite rules to child nodes.
    fn apply_rewrite_rules_to_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.apply_rewrite_rules(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    /// Recursively applies join optimization to child nodes.
    fn optimize_joins_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.optimize_joins(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    /// Recursively applies sort/limit optimization to child nodes.
    fn optimize_sort_and_limit_children(
        &self,
        mut plan: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        let mut new_children = Vec::new();
        for child in plan.children.drain(..) {
            new_children.push(self.optimize_sort_and_limit(child)?);
        }
        plan.children = new_children;
        Ok(plan)
    }

    /// Pushes a filter predicate as close to the data source as possible.
    ///
    /// Implements predicate pushdown by recursively moving the filter down
    /// through the plan tree. When encountering another filter, combines
    /// predicates using AND.
    ///
    /// # Arguments
    ///
    /// * `predicate` - The filter predicate to push down
    /// * `child` - The child plan to push the predicate into
    ///
    /// # Returns
    ///
    /// A new plan with the predicate pushed as close to the source as possible.
    fn push_down_predicate(
        &self,
        predicate: Arc<Expression>,
        child: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        match child.plan_type {
            LogicalPlanType::TableScan { .. } => {
                // Create a new filter node above the scan
                Ok(Box::new(LogicalPlan::new(
                    LogicalPlanType::Filter {
                        schema: Default::default(),
                        output_schema: Default::default(),
                        table_oid: 0,
                        table_name: "".to_string(),
                        predicate,
                    },
                    vec![child],
                )))
            },
            LogicalPlanType::Filter {
                schema: _,
                output_schema: _,
                table_oid: _,
                table_name: _,
                predicate: existing_pred,
            } => {
                // Combine predicates using AND
                let combined_pred = Arc::new(Expression::Logic(LogicExpression::new(
                    predicate.clone(),
                    existing_pred.clone(),
                    LogicType::And,
                    vec![],
                )));
                if let Some(grandchild) = child.children.first() {
                    self.push_down_predicate(combined_pred, grandchild.clone())
                } else {
                    Ok(Box::new(LogicalPlan::new(
                        LogicalPlanType::Filter {
                            schema: Default::default(),
                            output_schema: Default::default(),
                            table_oid: 0,
                            table_name: "".to_string(),
                            predicate: combined_pred,
                        },
                        vec![],
                    )))
                }
            },
            _ => Ok(Box::new(LogicalPlan::new(
                LogicalPlanType::Filter {
                    schema: Default::default(),
                    output_schema: Default::default(),
                    table_oid: 0,
                    table_name: "".to_string(),
                    predicate,
                },
                vec![child],
            ))),
        }
    }

    /// Attempts to merge consecutive projection nodes.
    ///
    /// When two projections are stacked, this method tries to combine them
    /// into a single projection by rewriting the outer expressions in terms
    /// of the inner ones.
    ///
    /// # Arguments
    ///
    /// * `expressions` - Expressions from the outer projection
    /// * `child` - The child plan (potentially another projection)
    ///
    /// # Returns
    ///
    /// A merged projection or the original structure if merging isn't beneficial.
    ///
    /// # Note
    ///
    /// Full projection merging logic is TODO - currently returns the child as-is
    /// if it's a projection.
    fn merge_projections(
        &self,
        expressions: Vec<Arc<Expression>>,
        child: Box<LogicalPlan>,
    ) -> Result<Box<LogicalPlan>, DBError> {
        if let LogicalPlanType::Projection { .. } = child.clone().plan_type {
            // TODO: Implement projection merging logic
            // This would involve rewriting the outer expressions in terms of the inner ones
            Ok(child)
        } else {
            // Calculate column mappings
            let input_schema = child.get_schema().unwrap();
            let mut column_mappings = Vec::with_capacity(expressions.len());

            for expr in &expressions {
                if let Expression::ColumnRef(col_ref) = expr.as_ref() {
                    let col_name = col_ref.get_return_type().get_name();

                    // Try to find the column in the input schema
                    let input_idx = input_schema
                        .get_columns()
                        .iter()
                        .position(|c| c.get_name() == col_name)
                        .unwrap_or(0); // Default to first column if not found

                    column_mappings.push(input_idx);
                } else {
                    // For non-column references, use a placeholder mapping
                    column_mappings.push(0);
                }
            }

            // TODO: Compute correct schema
            let output_schema = Schema::new(vec![]);

            Ok(Box::new(LogicalPlan::new(
                LogicalPlanType::Projection {
                    expressions,
                    schema: output_schema,
                    column_mappings,
                },
                vec![child],
            )))
        }
    }

    /// Determines whether to convert a nested loop join to a hash join.
    ///
    /// Evaluates the join predicate to decide if hash join would be more
    /// efficient than nested loop join.
    ///
    /// # Factors Considered
    ///
    /// - Is the join predicate an equality comparison?
    /// - Are input relations large enough to justify hash table overhead?
    /// - Are statistics available about input cardinalities?
    ///
    /// # Returns
    ///
    /// `true` if hash join should be used, `false` to keep nested loop join.
    ///
    /// # Note
    ///
    /// Currently always returns `false` - full implementation is TODO.
    fn should_use_hash_join(&self, _predicate: &Expression) -> bool {
        // TODO: Implement logic to determine if hash join would be beneficial
        // Consider factors like:
        // - Is the join predicate an equality comparison?
        // - Are the input relations large enough to justify hash join overhead?
        // - Are there available statistics about the input relations?
        false
    }

    /// Validates the optimized plan for correctness.
    ///
    /// Performs post-optimization validation to ensure the plan is executable:
    ///
    /// | Node Type | Validation |
    /// |-----------|------------|
    /// | `TableScan` | Table exists in catalog |
    /// | `NestedLoopJoin` | Has predicate (if NLJ check enabled) |
    /// | `TopN` | k > 0 and has sort specifications |
    ///
    /// # Arguments
    ///
    /// * `plan` - The optimized plan to validate
    /// * `check_options` - Flags controlling which validations to apply
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Plan is valid
    /// * `Err(DBError)` - Validation failed
    fn validate_plan(
        &self,
        plan: &LogicalPlan,
        check_options: &CheckOptions,
    ) -> Result<(), DBError> {
        trace!("Validating plan node: {:?}", plan.plan_type);

        match &plan.plan_type {
            LogicalPlanType::TableScan {
                table_name,
                table_oid,
                ..
            } => {
                let catalog = self.catalog.read();
                if catalog.get_table_by_oid(*table_oid).is_none() {
                    warn!("Table with OID {} not found in catalog", table_oid);
                    return Err(DBError::TableNotFound(table_name.clone()));
                }
            },
            LogicalPlanType::NestedLoopJoin { predicate, .. } => {
                if check_options.has_check(&CheckOption::EnableNljCheck)
                    && predicate.get_children().is_empty()
                {
                    warn!("NLJ validation failed: missing join predicate");
                    return Err(DBError::Validation(
                        "NLJ requires join predicate".to_string(),
                    ));
                }
            },
            LogicalPlanType::TopN {
                k,
                sort_specifications,
                ..
            } => {
                if *k == 0 || sort_specifications.is_empty() {
                    warn!("TopN validation failed: invalid specification");
                    return Err(DBError::Validation(
                        "Invalid TopN specification".to_string(),
                    ));
                }
            },
            _ => {},
        }

        // Recursively validate children
        for child in &plan.children {
            self.validate_plan(child, check_options)?;
        }

        trace!("Plan validation successful for node: {:?}", plan.plan_type);
        Ok(())
    }
}
