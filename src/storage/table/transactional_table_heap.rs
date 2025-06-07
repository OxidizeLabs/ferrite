use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::common::config::Timestamp;
use crate::common::config::INVALID_PAGE_ID;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockMode;
use crate::concurrency::transaction::UndoLog;
use crate::concurrency::transaction::{IsolationLevel, Transaction, TransactionState, UndoLink};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use crate::types_db::value::Value;
use log;
use std::sync::Arc;
use crate::types_db::types::Type;

#[derive(Debug, Clone)]
pub struct TransactionalTableHeap {
    table_heap: Arc<TableHeap>,
    table_oid: TableOidT,
}

impl TransactionalTableHeap {
    pub fn new(table_heap: Arc<TableHeap>, table_oid: TableOidT) -> Self {
        Self {
            table_heap,
            table_oid,
        }
    }

    pub fn get_table_oid(&self) -> TableOidT {
        self.table_oid
    }

    pub fn get_table_heap(&self) -> Arc<TableHeap> {
        self.table_heap.clone()
    }

    pub fn rollback_tuple(
        &self,
        meta: Arc<TupleMeta>,
        tuple: &Tuple,
        rid: RID,
    ) -> Result<(), String> {
        self.table_heap
            .update_tuple(meta, tuple, rid, None)
            .map(|_| ())
    }

    /// Insert a tuple from values and schema directly
    ///
    /// # Parameters
    ///
    /// - `values`: Values for the tuple.
    /// - `schema`: Schema for the tuple.
    /// - `txn_ctx`: Transaction context.
    ///
    /// # Returns
    ///
    /// An `Result` containing the RID of the inserted tuple, or an error message.
    pub fn insert_tuple_from_values(
        &self,
        values: Vec<Value>,
        schema: &Schema,
        txn_ctx: Arc<TransactionContext>,
    ) -> Result<RID, String> {
        let txn = txn_ctx.get_transaction();

        // Transaction checks
        if txn.get_state() != TransactionState::Running {
            return Err("Transaction not in running state".to_string());
        }

        // Lock acquisition
        let lock_manager = txn_ctx.get_lock_manager();
        lock_manager
            .lock_table(txn.clone(), LockMode::IntentionExclusive, self.table_oid)
            .map_err(|e| format!("Failed to acquire table lock: {}", e))?;

        // First expand values to handle AUTO_INCREMENT and DEFAULT values
        // This ensures we have the complete tuple values before constraint validation
        let expanded_values = self.table_heap.expand_values_for_schema(values, schema)?;

        // Validate check constraints on the expanded values
        Self::validate_check_constraints(&expanded_values, schema)?;
        
        // Validate PRIMARY KEY and UNIQUE constraints on the expanded values
        self.validate_primary_key_and_unique_constraints(&expanded_values, schema, &txn_ctx)?;

        // Create metadata with transaction ID
        let meta = Arc::new(TupleMeta::new(txn.get_transaction_id()));

        // Perform insert using the new internal method with expanded values
        let rid = self
            .table_heap
            .insert_tuple_from_values(expanded_values, schema, meta)?;

        // Transaction bookkeeping
        txn.append_write_set(self.table_oid, rid);

        Ok(rid)
    }

    /// Validates check constraints for the given values against the schema
    fn validate_check_constraints(values: &[Value], schema: &Schema) -> Result<(), String> {
        for (i, column) in schema.get_columns().iter().enumerate() {
            if let Some(constraint_expr) = column.get_check_constraint() {
                if i >= values.len() {
                    return Err(format!(
                        "Value index {} out of bounds for constraint validation",
                        i
                    ));
                }

                let value = &values[i];

                // For now, implement basic constraint validation for common patterns
                // This can be extended to support full SQL expression parsing later
                if !Self::evaluate_simple_constraint(constraint_expr, value, column.get_name())? {
                    return Err(format!(
                        "Check constraint violation for column '{}': {} (constraint: {})",
                        column.get_name(),
                        value,
                        constraint_expr
                    ));
                }
            }
        }

        Ok(())
    }

    /// Evaluates simple check constraints without requiring full SQL parsing
    fn evaluate_simple_constraint(
        constraint: &str,
        value: &Value,
        column_name: &str,
    ) -> Result<bool, String> {
        let constraint = constraint.trim();

        // Handle age >= 18 AND age <= 100 pattern
        if constraint.contains(" AND ") {
            let parts: Vec<&str> = constraint.split(" AND ").collect();
            for part in parts {
                if !Self::evaluate_simple_constraint(part.trim(), value, column_name)? {
                    return Ok(false);
                }
            }
            return Ok(true);
        }

        // Handle OR patterns
        if constraint.contains(" OR ") {
            let parts: Vec<&str> = constraint.split(" OR ").collect();
            for part in parts {
                if Self::evaluate_simple_constraint(part.trim(), value, column_name)? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }

        // Handle simple comparison operators
        if constraint.contains(">=") {
            let parts: Vec<&str> = constraint.split(">=").collect();
            if parts.len() == 2 {
                let left = parts[0].trim();
                let right = parts[1].trim();
                
                if left == column_name {
                    if let Ok(threshold) = right.parse::<f64>() {
                        if let Ok(val) = value.as_decimal() {
                            return Ok(val >= threshold);
                        } else if let Ok(val) = value.as_integer() {
                            return Ok(val as f64 >= threshold);
                        }
                    }
                }
            }
        }

        if constraint.contains("<=") {
            let parts: Vec<&str> = constraint.split("<=").collect();
            if parts.len() == 2 {
                let left = parts[0].trim();
                let right = parts[1].trim();
                
                if left == column_name {
                    if let Ok(threshold) = right.parse::<f64>() {
                        if let Ok(val) = value.as_decimal() {
                            return Ok(val <= threshold);
                        } else if let Ok(val) = value.as_integer() {
                            return Ok(val as f64 <= threshold);
                        }
                    }
                }
            }
        }

        if constraint.contains(" > ") {
            let parts: Vec<&str> = constraint.split(" > ").collect();
            if parts.len() == 2 {
                let left = parts[0].trim();
                let right = parts[1].trim();
                
                if left == column_name {
                    if let Ok(threshold) = right.parse::<f64>() {
                        if let Ok(val) = value.as_decimal() {
                            return Ok(val > threshold);
                        } else if let Ok(val) = value.as_integer() {
                            return Ok(val as f64 > threshold);
                        }
                    }
                }
            }
        }

        if constraint.contains(" < ") {
            let parts: Vec<&str> = constraint.split(" < ").collect();
            if parts.len() == 2 {
                let left = parts[0].trim();
                let right = parts[1].trim();
                
                if left == column_name {
                    if let Ok(threshold) = right.parse::<f64>() {
                        if let Ok(val) = value.as_decimal() {
                            return Ok(val < threshold);
                        } else if let Ok(val) = value.as_integer() {
                            return Ok((val as f64) < threshold);
                        }
                    }
                }
            }
        }

        if constraint.contains(" = ") {
            let parts: Vec<&str> = constraint.split(" = ").collect();
            if parts.len() == 2 {
                let left = parts[0].trim();
                let right = parts[1].trim();
                
                if left == column_name {
                    if let Ok(threshold) = right.parse::<f64>() {
                        if let Ok(val) = value.as_decimal() {
                            return Ok((val - threshold).abs() < f64::EPSILON);
                        } else if let Ok(val) = value.as_integer() {
                            return Ok((val as f64 - threshold).abs() < f64::EPSILON);
                        }
                    }
                }
            }
        }

        // If we can't parse the constraint, return true (allow the insert)
        // In a production system, you might want to be more strict here
        log::warn!(
            "Unable to evaluate check constraint '{}' for column '{}', allowing insert",
            constraint,
            column_name
        );
        Ok(true)
    }

    /// Validates PRIMARY KEY and UNIQUE constraints for the given values against existing data
    fn validate_primary_key_and_unique_constraints(
        &self,
        values: &[Value],
        schema: &Schema,
        txn_ctx: &Arc<TransactionContext>,
    ) -> Result<(), String> {
        // Collect primary key columns and their values
        let mut primary_key_values = Vec::new();
        let mut primary_key_columns = Vec::new();
        
        // Collect unique columns and their values (only for columns that are UNIQUE but not part of PRIMARY KEY)
        let mut unique_constraints = Vec::new();
        
        for (i, column) in schema.get_columns().iter().enumerate() {
            if i >= values.len() {
                return Err(format!(
                    "Value index {} out of bounds for constraint validation",
                    i
                ));
            }

            if column.is_primary_key() {
                primary_key_values.push(values[i].clone());
                primary_key_columns.push(column.get_name().to_string());
            }
            
            // Only check individual UNIQUE constraints for columns that are NOT part of the PRIMARY KEY
            // For PRIMARY KEY columns, uniqueness is checked as part of the composite key
            if column.is_unique() && !column.is_primary_key() {
                unique_constraints.push((column.get_name().to_string(), values[i].clone()));
            }
        }

        // Check PRIMARY KEY constraint if there are primary key columns
        if !primary_key_values.is_empty() {
            if self.check_primary_key_violation(&primary_key_values, &primary_key_columns, schema, txn_ctx)? {
                return Err(format!(
                    "Primary key violation: duplicate key value for columns ({})",
                    primary_key_columns.join(", ")
                ));
            }
        }

        // Check UNIQUE constraints for non-primary key columns
        for (column_name, value) in unique_constraints {
            if self.check_unique_constraint_violation(&column_name, &value, schema, txn_ctx)? {
                return Err(format!(
                    "Unique constraint violation for column '{}': value '{}' already exists",
                    column_name,
                    value
                ));
            }
        }

        Ok(())
    }

    /// Check if inserting the given primary key values would violate the PRIMARY KEY constraint
    fn check_primary_key_violation(
        &self,
        pk_values: &[Value],
        pk_columns: &[String],
        schema: &Schema,
        txn_ctx: &Arc<TransactionContext>,
    ) -> Result<bool, String> {
        // Create an iterator over all tuples in the table
        let iterator = self.make_iterator(Some(txn_ctx.clone()));
        
        for item in iterator {
            let (_, tuple) = item;
            
            // Check if this tuple has the same primary key values
            let mut matches = true;
            for (i, pk_column) in pk_columns.iter().enumerate() {
                if let Some(column_index) = Self::find_column_index(schema, pk_column) {
                    let existing_value = tuple.get_value(column_index);
                    if existing_value != pk_values[i] {
                        matches = false;
                        break;
                    }
                } else {
                    return Err(format!("Primary key column '{}' not found in schema", pk_column));
                }
            }
            
            if matches {
                return Ok(true); // Found a duplicate
            }
        }
        
        Ok(false) // No duplicate found
    }

    /// Check if inserting the given value would violate a UNIQUE constraint
    fn check_unique_constraint_violation(
        &self,
        column_name: &str,
        value: &Value,
        schema: &Schema,
        txn_ctx: &Arc<TransactionContext>,
    ) -> Result<bool, String> {
        let column_index = Self::find_column_index(schema, column_name)
            .ok_or_else(|| format!("Unique constraint column '{}' not found in schema", column_name))?;
        
        // Create an iterator over all tuples in the table
        let iterator = self.make_iterator(Some(txn_ctx.clone()));
        
        for item in iterator {
            let (_, tuple) = item;
            
            let existing_value = tuple.get_value(column_index);
            if existing_value == *value {
                return Ok(true); // Found a duplicate
            }
        }
        
        Ok(false) // No duplicate found
    }

    /// Helper function to find the index of a column by name
    fn find_column_index(schema: &Schema, column_name: &str) -> Option<usize> {
        schema.get_columns()
            .iter()
            .position(|col| col.get_name() == column_name)
    }

    pub fn insert_tuple(
        &self,
        meta: Arc<TupleMeta>,
        tuple: &Tuple,
        txn_ctx: Arc<TransactionContext>,
    ) -> Result<RID, String> {
        let txn = txn_ctx.get_transaction();

        // Transaction checks
        if txn.get_state() != TransactionState::Running {
            return Err("Transaction not in running state".to_string());
        }

        // Lock acquisition
        let lock_manager = txn_ctx.get_lock_manager();
        lock_manager
            .lock_table(txn.clone(), LockMode::IntentionExclusive, self.table_oid)
            .map_err(|e| format!("Failed to acquire table lock: {}", e))?;

        // Perform insert using internal method
        let rid = self.table_heap.insert_tuple_internal(meta, tuple)?;

        // Transaction bookkeeping
        txn.append_write_set(self.table_oid, rid);

        Ok(rid)
    }

    pub fn get_tuple(
        &self,
        rid: RID,
        txn_ctx: Arc<TransactionContext>,
    ) -> Result<(Arc<TupleMeta>, Arc<Tuple>), String> {
        let txn = txn_ctx.get_transaction();
        let txn_manager = txn_ctx.get_transaction_manager();

        // Get latest version
        let (meta, tuple) = self.table_heap.get_tuple_internal(rid)?;

        // Handle visibility based on isolation level
        match txn.get_isolation_level() {
            IsolationLevel::ReadUncommitted => Ok((meta, tuple)),
            IsolationLevel::ReadCommitted => {
                if meta.is_committed() || meta.get_creator_txn_id() == txn.get_transaction_id() {
                    Ok((meta, tuple))
                } else {
                    Err("Tuple not visible".to_string())
                }
            }
            _ => self.get_visible_version(rid, txn, txn_manager, meta, tuple),
        }
    }

    pub fn update_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &Tuple,
        rid: RID,
        txn_ctx: Arc<TransactionContext>,
    ) -> Result<RID, String> {
        let txn = txn_ctx.get_transaction();

        // Transaction checks
        if txn.get_state() != TransactionState::Running {
            return Err("Transaction not in running state".to_string());
        }

        // Get current version for visibility check
        let (current_meta, current_tuple) = self.table_heap.get_tuple_internal(rid)?;

        // Visibility checks
        if current_meta.get_creator_txn_id() != meta.get_creator_txn_id()
            && !current_meta.is_committed()
        {
            return Err("Cannot update uncommitted tuple from another transaction".to_string());
        }

        // Create undo log with link to current version
        let txn_manager = txn_ctx.get_transaction_manager();
        log::debug!(
            "Creating undo log for update. Current version: creator_txn={}, commit_ts={}",
            current_meta.get_creator_txn_id(),
            current_meta.get_commit_timestamp()
        );

        // Create undo log that points to the original version
        let undo_log = UndoLog {
            is_deleted: false,
            modified_fields: vec![true; tuple.get_column_count()],
            tuple: current_tuple,
            ts: current_meta.get_commit_timestamp(),
            prev_version: UndoLink {
                prev_txn: current_meta.get_creator_txn_id(),
                prev_log_idx: current_meta.get_undo_log_idx(),
            },
        };

        // Append undo log and get link
        let undo_link = txn.append_undo_log(Arc::from(undo_log));
        log::debug!(
            "Appended undo log. New link: prev_txn={}, prev_log_idx={}, txn_id={}, log_num={}",
            undo_link.prev_txn,
            undo_link.prev_log_idx,
            txn.get_transaction_id(),
            txn.get_undo_log_num()
        );

        // Update version chain - preserve the current version's link
        let current_version_link = txn_manager.get_undo_link(rid);
        log::debug!(
            "Current version link before update: {:?}, txn_id={}, current_meta: creator={}, idx={}",
            current_version_link,
            txn.get_transaction_id(),
            current_meta.get_creator_txn_id(),
            current_meta.get_undo_log_idx()
        );

        // Update the undo link to point to the current version
        txn_manager.update_undo_link(rid, Some(undo_link), None);

        // Create new meta with current transaction as creator
        let mut new_meta = TupleMeta::new(txn.get_transaction_id());
        new_meta.set_commit_timestamp(Timestamp::MAX);
        new_meta.set_undo_log_idx(txn.get_undo_log_num() - 1);

        log::debug!(
            "Created new meta: creator={}, idx={}, commit_ts={}",
            new_meta.get_creator_txn_id(),
            new_meta.get_undo_log_idx(),
            new_meta.get_commit_timestamp()
        );

        // Perform update
        let result = self
            .table_heap
            .update_tuple_internal(Arc::from(new_meta), tuple, rid)?;
        txn.append_write_set(self.table_oid, rid);

        Ok(result)
    }

    // Helper method for REPEATABLE_READ and SERIALIZABLE
    fn get_visible_version(
        &self,
        rid: RID,
        txn: Arc<Transaction>,
        txn_manager: Arc<TransactionManager>,
        meta: Arc<TupleMeta>,
        tuple: Arc<Tuple>,
    ) -> Result<(Arc<TupleMeta>, Arc<Tuple>), String> {
        log::debug!(
            "Starting version chain traversal for RID {:?} - Latest version: creator_txn={}, commit_ts={}, txn_id={}",
            rid,
            meta.get_creator_txn_id(),
            meta.get_commit_timestamp(),
            txn.get_transaction_id()
        );

        // First check if latest version is visible
        if txn.is_tuple_visible(&meta) {
            log::debug!("Latest version is visible, returning it");
            return Ok((meta, tuple));
        }

        // If latest version isn't visible, check version chain
        let mut current_meta = meta;
        let mut current_tuple = tuple;
        let mut current_link = txn_manager.get_undo_link(rid);

        log::debug!(
            "Latest version not visible, starting chain traversal. Initial undo link: {:?}",
            current_link
        );

        while let Some(ref undo_link) = current_link {
            log::debug!(
                "Examining version: prev_txn={}, prev_log_idx={}, txn_id={}",
                undo_link.prev_txn,
                undo_link.prev_log_idx,
                txn.get_transaction_id()
            );

            let undo_log = txn_manager.get_undo_log(undo_link.clone());
            let undo_log_clone = undo_log.clone();
            log::debug!(
                "Retrieved undo log: ts={}, is_deleted={}, prev_version: txn={}, idx={}",
                undo_log.ts,
                undo_log.is_deleted,
                undo_log.prev_version.prev_txn,
                undo_log.prev_version.prev_log_idx
            );

            // Create metadata for previous version using the undo log's prev_version
            let mut prev_meta = TupleMeta::new(undo_log.prev_version.prev_txn);
            prev_meta.set_commit_timestamp(undo_log.ts);
            prev_meta.set_deleted(undo_log.is_deleted);
            prev_meta.set_undo_log_idx(undo_log.prev_version.prev_log_idx);

            log::debug!(
                "Created previous version metadata: creator_txn={}, commit_ts={}, deleted={}, undo_idx={}",
                prev_meta.get_creator_txn_id(),
                prev_meta.get_commit_timestamp(),
                prev_meta.is_deleted(),
                prev_meta.get_undo_log_idx()
            );

            // Update current version
            current_meta = Arc::from(prev_meta);
            current_tuple = undo_log.tuple.clone();

            if txn.is_tuple_visible(&current_meta) {
                log::debug!(
                    "Found visible version: creator_txn={}, commit_ts={}",
                    current_meta.get_creator_txn_id(),
                    current_meta.get_commit_timestamp()
                );
                return Ok((current_meta, current_tuple));
            }

            // Move to next version in chain using the undo log's prev_version
            current_link = if undo_log.prev_version.is_valid() {
                Some(undo_log_clone.prev_version.clone())
            } else {
                None
            };
        }

        log::debug!("No visible version found in chain");
        Err("No visible version found".to_string())
    }

    pub fn make_iterator(&self, txn_ctx: Option<Arc<TransactionContext>>) -> TableIterator {
        let first_page_id = self.table_heap.get_first_page_id();

        TableIterator::new(
            Arc::new(self.clone()),
            RID::new(first_page_id, 0),
            RID::new(INVALID_PAGE_ID, 0),
            txn_ctx,
        )
    }

    /// Marks a tuple as deleted without actually removing it
    /// This is used during transaction abort to invalidate inserted tuples
    pub fn mark_tuple_deleted(&self, meta: &mut TupleMeta) -> Result<(), String> {
        // Update the metadata to mark as deleted
        meta.set_deleted(true);
        meta.set_creator_txn_id(meta.get_creator_txn_id());
        Ok(())
    }

    /// Deletes a tuple at the given RID by marking it as deleted
    pub fn delete_tuple(&self, rid: RID, txn_ctx: Arc<TransactionContext>) -> Result<(), String> {
        let txn = txn_ctx.get_transaction();

        // Transaction checks
        if txn.get_state() != TransactionState::Running {
            return Err("Transaction not in running state".to_string());
        }

        // Lock acquisition
        // let lock_manager = txn_ctx.get_lock_manager();
        // lock_manager.lock_table(txn.clone(), LockMode::IntentionExclusive, self.table_oid)
        //     .map_err(|e| format!("Failed to acquire table lock: {}", e))?;

        // Get current version for visibility check
        let (current_meta, current_tuple) = self.table_heap.get_tuple_internal(rid)?;

        // Visibility checks
        if current_meta.get_creator_txn_id() != txn.get_transaction_id()
            && !current_meta.is_committed()
        {
            return Err("Cannot delete uncommitted tuple from another transaction".to_string());
        }

        // Create undo log with link to current version
        let txn_manager = txn_ctx.get_transaction_manager();
        log::debug!(
            "Creating undo log for delete. Current version: creator_txn={}, commit_ts={}",
            current_meta.get_creator_txn_id(),
            current_meta.get_commit_timestamp()
        );

        // Create undo log that points to the original version
        let undo_log = UndoLog {
            is_deleted: false, // Original version was not deleted
            modified_fields: vec![true; current_tuple.get_column_count()],
            tuple: current_tuple.clone(),
            ts: current_meta.get_commit_timestamp(),
            prev_version: UndoLink {
                prev_txn: current_meta.get_creator_txn_id(),
                prev_log_idx: current_meta.get_undo_log_idx(),
            },
        };

        // Append undo log and get link
        let undo_link = txn.append_undo_log(Arc::from(undo_log));
        log::debug!(
            "Appended undo log. New link: prev_txn={}, prev_log_idx={}",
            undo_link.prev_txn,
            undo_link.prev_log_idx
        );

        // Update version chain
        txn_manager.update_undo_link(rid, Some(undo_link), None);

        // Create new meta with current transaction as creator
        let mut new_meta = TupleMeta::new(txn.get_transaction_id());
        new_meta.set_deleted(true); // Mark as deleted
        new_meta.set_commit_timestamp(Timestamp::MAX);
        new_meta.set_undo_log_idx(txn.get_undo_log_num() - 1);

        log::debug!(
            "Created new meta: creator={}, idx={}, commit_ts={}, deleted=true",
            new_meta.get_creator_txn_id(),
            new_meta.get_undo_log_idx(),
            new_meta.get_commit_timestamp()
        );

        // Update the tuple with deleted metadata
        self.table_heap
            .update_tuple_internal(Arc::from(new_meta), &current_tuple, rid)?;
        txn.append_write_set(self.table_oid, rid);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::concurrency::lock_manager::LockManager;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use parking_lot::RwLock;
    use tempfile::TempDir;

    struct TestContext {
        txn_heap: Arc<TransactionalTableHeap>,
        txn_manager: Arc<TransactionManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            // Set up storage components
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, 2)));
            let bpm = Arc::new(BufferPoolManager::new(
                100,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            // Set up transaction components with mock lock manager
            let txn_manager = Arc::new(TransactionManager::new());

            // Create table heap
            let table_heap = Arc::new(TableHeap::new(bpm.clone(), 1));

            // Create transactional table heap
            let txn_heap = Arc::new(TransactionalTableHeap::new(table_heap.clone(), 1));

            Self {
                txn_heap,
                txn_manager,
                _temp_dir: temp_dir,
            }
        }

        fn create_transaction_context(
            &self,
            isolation_level: IsolationLevel,
        ) -> Arc<TransactionContext> {
            let txn = self.txn_manager.begin(isolation_level).unwrap();
            Arc::new(TransactionContext::new(
                txn,
                Arc::new(LockManager::new()), // Use mock lock manager
                self.txn_manager.clone(),
            ))
        }

        fn create_test_schema() -> Schema {
            Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("value", TypeId::Integer),
            ])
        }
    }

    fn create_test_tuple() -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        let values = vec![Value::new(1), Value::new(100)];
        let tuple = Tuple::new(&values, &schema, RID::new(0, 0));
        let meta = TupleMeta::new(0); // Initialize with 0, but this will be overwritten
        (meta, tuple)
    }

    #[test]
    fn test_insert_tuple_from_values() {
        let ctx = TestContext::new("test_insert_tuple_from_values");

        // Create transaction
        let txn_ctx = ctx.create_transaction_context(IsolationLevel::ReadCommitted);

        // Create schema
        let schema = TestContext::create_test_schema();

        // Create values
        let values = vec![Value::new(1), Value::new(100)];

        // Insert tuple directly from values
        let rid = ctx
            .txn_heap
            .insert_tuple_from_values(values.clone(), &schema, txn_ctx.clone())
            .expect("Insert from values failed");

        // Verify the tuple was inserted correctly
        let (meta, tuple) = ctx
            .txn_heap
            .get_tuple(rid, txn_ctx.clone())
            .expect("Failed to get tuple");

        assert_eq!(tuple.get_value(0), Value::new(1));
        assert_eq!(tuple.get_value(1), Value::new(100));
        assert_eq!(
            meta.get_creator_txn_id(),
            txn_ctx.get_transaction().get_transaction_id()
        );
    }

    #[test]
    fn test_version_chain_with_values() {
        let ctx = TestContext::new("test_version_chain_with_values");

        // Create first transaction
        let txn_ctx1 = ctx.create_transaction_context(IsolationLevel::ReadCommitted);
        let txn1 = txn_ctx1.get_transaction();

        // Create schema
        let schema = TestContext::create_test_schema();

        // Create values
        let values = vec![Value::new(1), Value::new(100)];

        // Insert tuple directly from values
        let rid = ctx
            .txn_heap
            .insert_tuple_from_values(values.clone(), &schema, txn_ctx1.clone())
            .expect("Insert from values failed");

        // Commit first transaction
        ctx.txn_manager
            .commit(txn1.clone(), ctx.txn_heap.get_table_heap().get_bpm());

        // Create second transaction
        let txn_ctx2 = ctx.create_transaction_context(IsolationLevel::RepeatableRead);

        // Get the tuple to update
        let (meta, tuple) = ctx
            .txn_heap
            .get_tuple(rid, txn_ctx2.clone())
            .expect("Failed to get tuple");

        // Create a new tuple with modified values
        let mut values = tuple.get_values();
        values[1] = Value::new(200);
        let new_tuple = Tuple::new(&values, &schema, rid);

        ctx.txn_heap
            .update_tuple(&meta, &new_tuple, rid, txn_ctx2.clone())
            .expect("Update failed");

        // Create third transaction to verify version chain
        let txn_ctx3 = ctx.create_transaction_context(IsolationLevel::RepeatableRead);
        let (old_meta, old_tuple) = ctx
            .txn_heap
            .get_tuple(rid, txn_ctx3)
            .expect("Failed to get old version");

        assert_eq!(
            old_meta.get_creator_txn_id(),
            txn1.get_transaction_id(),
            "Creator transaction ID mismatch - expected {}, got {}",
            txn1.get_transaction_id(),
            old_meta.get_creator_txn_id()
        );
        assert_eq!(old_tuple.get_value(1), Value::new(100));
    }

    #[test]
    fn test_version_chain() {
        let ctx = TestContext::new("test_version_chain");

        // Create first transaction
        let txn_ctx1 = ctx.create_transaction_context(IsolationLevel::ReadCommitted);
        let txn1 = txn_ctx1.get_transaction();

        // Insert initial version with txn1's ID
        let (mut meta, mut tuple) = create_test_tuple();
        meta = TupleMeta::new(txn1.get_transaction_id());

        let rid = ctx
            .txn_heap
            .insert_tuple(Arc::from(meta), &mut tuple, txn_ctx1.clone())
            .expect("Insert failed");

        // Commit first transaction
        ctx.txn_manager
            .commit(txn1.clone(), ctx.txn_heap.get_table_heap().get_bpm());

        // Create second transaction
        let txn_ctx2 = ctx.create_transaction_context(IsolationLevel::RepeatableRead);

        // Update tuple with txn2
        let mut new_tuple = tuple;
        new_tuple.get_values_mut()[1] = Value::new(200);

        ctx.txn_heap
            .update_tuple(&meta, &mut new_tuple, rid, txn_ctx2.clone())
            .expect("Update failed");

        // Create third transaction to verify version chain
        let txn_ctx3 = ctx.create_transaction_context(IsolationLevel::RepeatableRead);
        let (old_meta, old_tuple) = ctx
            .txn_heap
            .get_tuple(rid, txn_ctx3)
            .expect("Failed to get old version");

        assert_eq!(
            old_meta.get_creator_txn_id(),
            txn1.get_transaction_id(),
            "Creator transaction ID mismatch - expected {}, got {}",
            txn1.get_transaction_id(),
            old_meta.get_creator_txn_id()
        );
        assert_eq!(old_tuple.get_values()[1], Value::new(100));
    }

    #[test]
    fn test_isolation_levels_with_values() {
        let ctx = TestContext::new("test_isolation_levels_with_values");

        // Create READ_UNCOMMITTED transaction
        let txn_ctx1 = ctx.create_transaction_context(IsolationLevel::ReadUncommitted);

        // Create schema and values
        let schema = TestContext::create_test_schema();
        let values = vec![Value::new(1), Value::new(100)];

        // Insert tuple using the values API
        let rid = ctx
            .txn_heap
            .insert_tuple_from_values(values, &schema, txn_ctx1.clone())
            .expect("Insert from values failed");

        // Verify visibility based on isolation level
        let txn_ctx2 = ctx.create_transaction_context(IsolationLevel::ReadCommitted);
        assert!(
            ctx.txn_heap.get_tuple(rid, txn_ctx2).is_err(),
            "READ_COMMITTED should not see uncommitted tuple"
        );

        let txn_ctx3 = ctx.create_transaction_context(IsolationLevel::ReadUncommitted);
        assert!(
            ctx.txn_heap.get_tuple(rid, txn_ctx3).is_ok(),
            "READ_UNCOMMITTED should see uncommitted tuple"
        );
    }

    #[test]
    fn test_isolation_levels() {
        let ctx = TestContext::new("test_isolation_levels");

        // Create READ_UNCOMMITTED transaction
        let txn_ctx1 = ctx.create_transaction_context(IsolationLevel::ReadUncommitted);
        let txn1 = txn_ctx1.get_transaction();

        // Insert tuple
        let (mut meta, mut tuple) = create_test_tuple();
        meta = TupleMeta::new(txn1.get_transaction_id());

        let rid = ctx
            .txn_heap
            .insert_tuple(Arc::from(meta), &mut tuple, txn_ctx1.clone())
            .expect("Insert failed");

        // Verify visibility based on isolation level
        let txn_ctx2 = ctx.create_transaction_context(IsolationLevel::ReadCommitted);
        assert!(
            ctx.txn_heap.get_tuple(rid, txn_ctx2).is_err(),
            "READ_COMMITTED should not see uncommitted tuple"
        );

        let txn_ctx3 = ctx.create_transaction_context(IsolationLevel::ReadUncommitted);
        assert!(
            ctx.txn_heap.get_tuple(rid, txn_ctx3).is_ok(),
            "READ_UNCOMMITTED should see uncommitted tuple"
        );
    }
}
