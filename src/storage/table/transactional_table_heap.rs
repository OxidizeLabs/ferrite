use crate::common::config::Timestamp;
use std::sync::Arc;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockMode;
use crate::concurrency::transaction::{IsolationLevel, Transaction, TransactionState, UndoLink};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use crate::common::config::TableOidT;
use crate::concurrency::transaction::UndoLog;
use log;
use crate::common::config::{INVALID_PAGE_ID};
use crate::storage::table::table_iterator::TableIterator;

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

    pub fn rollback_tuple(&self, meta: &TupleMeta, tuple: &mut Tuple, rid: RID) -> Result<(), String> {
        self.table_heap.update_tuple(meta, tuple, rid, None)
            .map(|_| ())
    }

    pub fn insert_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        txn_ctx: Arc<TransactionContext>,
    ) -> Result<RID, String> {
        let txn = txn_ctx.get_transaction();
        
        // Transaction checks
        if txn.get_state() != TransactionState::Running {
            return Err("Transaction not in running state".to_string());
        }

        // Lock acquisition
        let lock_manager = txn_ctx.get_lock_manager();
        lock_manager.lock_table(txn.clone(), LockMode::IntentionExclusive, self.table_oid)
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
    ) -> Result<(TupleMeta, Tuple), String> {
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
            _ => self.get_visible_version(rid, txn, txn_manager, meta, tuple)
        }
    }

    pub fn update_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
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
        if current_meta.get_creator_txn_id() != meta.get_creator_txn_id() && 
           !current_meta.is_committed() {
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
        let undo_link = txn.append_undo_log(undo_log);
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
        let result = self.table_heap.update_tuple_internal(&new_meta, tuple, rid)?;
        txn.append_write_set(self.table_oid, rid);

        Ok(result)
    }

    // Helper method for REPEATABLE_READ and SERIALIZABLE
    fn get_visible_version(
        &self,
        rid: RID,
        txn: Arc<Transaction>,
        txn_manager: Arc<TransactionManager>,
        meta: TupleMeta,
        tuple: Tuple,
    ) -> Result<(TupleMeta, Tuple), String> {
        log::debug!(
            "Starting version chain traversal for RID {:?} - Latest version: creator_txn={}, commit_ts={}, txn_id={}",
            rid, meta.get_creator_txn_id(), meta.get_commit_timestamp(), txn.get_transaction_id()
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
            current_meta = prev_meta;
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
                Some(undo_log.prev_version)
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
    pub fn delete_tuple(
        &self,
        rid: RID,
        txn_ctx: Arc<TransactionContext>,
    ) -> Result<(), String> {
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
        if current_meta.get_creator_txn_id() != txn.get_transaction_id() && 
           !current_meta.is_committed() {
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
        let undo_link = txn.append_undo_log(undo_log);
        log::debug!(
            "Appended undo log. New link: prev_txn={}, prev_log_idx={}",
            undo_link.prev_txn,
            undo_link.prev_log_idx
        );

        // Update version chain
        txn_manager.update_undo_link(rid, Some(undo_link), None);

        // Create new meta with current transaction as creator
        let mut new_meta = TupleMeta::new(txn.get_transaction_id());
        new_meta.set_deleted(true);  // Mark as deleted
        new_meta.set_commit_timestamp(Timestamp::MAX);
        new_meta.set_undo_log_idx(txn.get_undo_log_num() - 1);

        log::debug!(
            "Created new meta: creator={}, idx={}, commit_ts={}, deleted=true",
            new_meta.get_creator_txn_id(),
            new_meta.get_undo_log_idx(),
            new_meta.get_commit_timestamp()
        );

        // Update the tuple with deleted metadata
        self.table_heap.update_tuple(&new_meta, &mut current_tuple.clone(), rid, None)?;
        txn.append_write_set(self.table_oid, rid);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use parking_lot::RwLock;
    use tempfile::TempDir;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::concurrency::lock_manager::LockManager;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    struct TestContext {
        txn_heap: Arc<TransactionalTableHeap>,
        txn_manager: Arc<TransactionManager>,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            // Set up storage components
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir.path().join(format!("{name}.db")).to_str().unwrap().to_string();
            let log_path = temp_dir.path().join(format!("{name}.log")).to_str().unwrap().to_string();

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
            let txn_heap = Arc::new(TransactionalTableHeap::new(
                table_heap.clone(),
                1,
            ));

            Self {
                txn_heap,
                txn_manager,
                _temp_dir: temp_dir,
            }
        }

        fn create_transaction_context(&self, isolation_level: IsolationLevel) -> Arc<TransactionContext> {
            let txn = self.txn_manager.begin(isolation_level).unwrap();
            Arc::new(TransactionContext::new(
                txn,
                Arc::new(LockManager::new()),  // Use mock lock manager
                self.txn_manager.clone(),
            ))
        }
    }

    fn create_test_tuple() -> (TupleMeta, Tuple) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Integer),
        ]);
        let values = vec![Value::new(1), Value::new(100)];
        let tuple = Tuple::new(&values, schema, RID::new(0, 0));
        let meta = TupleMeta::new(0); // Initialize with 0, but this will be overwritten
        (meta, tuple)
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

        let rid = ctx.txn_heap.insert_tuple(&meta, &mut tuple, txn_ctx1.clone())
            .expect("Insert failed");

        // Commit first transaction
        ctx.txn_manager.commit(txn1.clone(), ctx.txn_heap.get_table_heap().get_bpm());

        // Create second transaction
        let txn_ctx2 = ctx.create_transaction_context(IsolationLevel::RepeatableRead);

        // Update tuple with txn2
        let mut new_tuple = tuple.clone();
        new_tuple.get_values_mut()[1] = Value::new(200);

        ctx.txn_heap.update_tuple(&meta, &mut new_tuple, rid, txn_ctx2.clone())
            .expect("Update failed");

        // Create third transaction to verify version chain
        let txn_ctx3 = ctx.create_transaction_context(IsolationLevel::RepeatableRead);
        let (old_meta, old_tuple) = ctx.txn_heap.get_tuple(rid, txn_ctx3)
            .expect("Failed to get old version");

        assert_eq!(old_meta.get_creator_txn_id(), txn1.get_transaction_id(),
                   "Creator transaction ID mismatch - expected {}, got {}",
                   txn1.get_transaction_id(), old_meta.get_creator_txn_id());
        assert_eq!(old_tuple.get_values()[1], Value::new(100));
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
        
        let rid = ctx.txn_heap.insert_tuple(&meta, &mut tuple, txn_ctx1.clone())
            .expect("Insert failed");

        // Verify visibility based on isolation level
        let txn_ctx2 = ctx.create_transaction_context(IsolationLevel::ReadCommitted);
        assert!(ctx.txn_heap.get_tuple(rid, txn_ctx2).is_err(),
                "READ_COMMITTED should not see uncommitted tuple");

        let txn_ctx3 = ctx.create_transaction_context(IsolationLevel::ReadUncommitted);
        assert!(ctx.txn_heap.get_tuple(rid, txn_ctx3).is_ok(),
                "READ_UNCOMMITTED should see uncommitted tuple");
    }
}