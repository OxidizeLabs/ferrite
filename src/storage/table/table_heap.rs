use crate::buffer::buffer_pool_manager::{BufferPoolManager, NewPageType};
use crate::catalog::schema::Schema;
use crate::common::config::{PageId, TableOidT, INVALID_PAGE_ID, INVALID_TXN_ID};
use crate::common::rid::RID;
use crate::concurrency::lock_manager::{LockManager, LockMode};
use crate::concurrency::transaction::{IsolationLevel, TransactionState};
use crate::concurrency::transaction_manager::TransactionManager;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::page::page::PageTrait;
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::table_page::TablePage;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use log::debug;
use parking_lot::RwLock;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

// Page Management
pub struct TablePageManager {
    bpm: Arc<BufferPoolManager>,
    first_page_id: PageId,
    last_page_id: PageId,
}

// Tuple Storage
pub struct TupleStorage {
    bpm: Arc<BufferPoolManager>,
    first_page_id: RwLock<PageId>,
    last_page_id: RwLock<PageId>,
    table_oid: TableOidT,
}

// Transaction Management
pub struct TableTransactionManager {
    lock_manager: Arc<LockManager>,
    transaction_manager: Arc<TransactionManager>,
    table_oid: TableOidT,
}

/// TableHeap represents a physical table on disk.
/// This is just a doubly-linked list of pages.
pub struct TableHeap {
    page_manager: Arc<TablePageManager>,
    tuple_storage: Arc<TupleStorage>,
    txn_manager: Arc<TableTransactionManager>,
    pub latch: RwLock<()>,
}

/// The TableInfo struct maintains metadata about a table.
#[derive(Debug, Clone)]
pub struct TableInfo {
    /// The table schema
    schema: Schema,
    /// The table name
    name: String,
    /// An owning pointer to the table heap
    table: Arc<TableHeap>,
    /// The table OID
    oid: TableOidT,
}

impl TableHeap {
    /// Creates a new table heap without a transaction (open table).
    ///
    /// # Parameters
    ///
    /// - `bpm`: Buffer pool manager.
    /// - `table_oid`: Table OID.
    /// - `txn_manager`: Transaction manager.
    ///
    /// # Returns
    ///
    /// A new `TableHeap` instance.
    pub fn new(
        bpm: Arc<BufferPoolManager>,
        table_oid: TableOidT,
        txn_manager: Arc<TransactionManager>,
    ) -> Self {
        let lock_manager = Arc::new(LockManager::new());
        let tuple_storage = TupleStorage::new(bpm.clone(), table_oid);

        Self {
            page_manager: Arc::new(TablePageManager {
                bpm: bpm.clone(),
                first_page_id: INVALID_PAGE_ID,
                last_page_id: INVALID_PAGE_ID,
            }),
            tuple_storage: Arc::new(tuple_storage),
            txn_manager: Arc::new(TableTransactionManager {
                lock_manager,
                transaction_manager: txn_manager,
                table_oid,
            }),
            latch: RwLock::new(()),
        }
    }

    /// Inserts a tuple into the table. If the tuple is too large (>= page_size), returns `None`.
    ///
    /// # Parameters
    ///
    /// - `meta`: Tuple meta.
    /// - `tuple`: Tuple to insert.
    /// - `lock_mgr`: Optional lock manager.
    /// - `txn`: Optional transaction.
    /// - `oid`: Table OID.
    ///
    /// # Returns
    ///
    /// An `Option` containing the RID of the inserted tuple, or `None` if the tuple is too large
    pub fn insert_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Result<RID, String> {
        self.tuple_storage.insert_tuple(meta, tuple, txn_ctx)
    }

    pub fn update_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        rid: RID,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Result<RID, String> {
        let _write_guard = self.latch.write();

        // First check transaction context and acquire locks if needed
        if let Some(txn_ctx) = &txn_ctx {
            let txn = txn_ctx.get_transaction();
            // Get the current tuple's metadata with transaction context
            let (current_meta, _) = self.get_tuple(rid, Some(txn_ctx.clone()))?;

            // Check for write-write conflicts
            if current_meta.get_creator_txn_id() != meta.get_creator_txn_id()
                && current_meta.get_creator_txn_id() != INVALID_TXN_ID
            {
                // Get transaction manager from context
                let txn_manager = txn_ctx.get_transaction_manager();

                // Check if the writing transaction is still active
                let writer_txn = txn_manager.get_transaction(&current_meta.get_creator_txn_id());
                if let Some(writer_txn) = writer_txn {
                    if writer_txn.get_state() == TransactionState::Running {
                        return Err(
                            "Write-write conflict: tuple is locked by another transaction"
                                .to_string(),
                        );
                    }
                }
            }

            // Acquire write lock through lock manager
            let lock_manager = txn_ctx.get_lock_manager();
            if let Err(e) = lock_manager.lock_table(txn, LockMode::Exclusive, self.get_table_oid())
            {
                return Err(format!("Failed to acquire write lock: {}", e));
            }
        }

        // Helper function to try updating in a page
        let mut try_update = |page_id: PageId| -> Option<Result<RID, String>> {
            if let Ok(page_guard) = self.tuple_storage.get_page(page_id) {
                if let Some(mut table_page_guard) = page_guard.into_specific_type::<TablePage, 8>()
                {
                    return Some(
                        table_page_guard
                            .access_mut(|table_page| {
                                match table_page.update_tuple(meta, tuple, rid) {
                                    Ok(()) => {
                                        // If update successful and we have transaction context, add to write set
                                        if let Some(txn_ctx) = &txn_ctx {
                                            debug!(
                                                "Adding page {} to transaction write set",
                                                page_id
                                            );
                                            txn_ctx
                                                .append_write_set_atomic(self.get_table_oid(), rid);
                                        }
                                        Ok(rid)
                                    }
                                    Err(_) => {
                                        // If update fails, try inserting as new tuple
                                        match table_page.insert_tuple(meta, tuple) {
                                            Some(new_rid) => {
                                                // Add new tuple to transaction write set
                                                if let Some(txn_ctx) = &txn_ctx {
                                                    txn_ctx.append_write_set_atomic(
                                                        self.get_table_oid(),
                                                        new_rid,
                                                    );
                                                }
                                                Ok(new_rid)
                                            }
                                            None => {
                                                Err("Failed to update/insert tuple".to_string())
                                            }
                                        }
                                    }
                                }
                            })
                            .unwrap_or_else(|| Err("Failed to access table page".to_string())),
                    );
                }
            }
            None
        };

        // Try updating in original page, last page, or new page as before

        if let Some(result) = try_update(rid.get_page_id()) {
            return result;
        }

        if let Some(result) = try_update(*self.tuple_storage.last_page_id.read()) {
            return result;
        }

        // Create new page and try there as last resort
        let new_page_id = self.tuple_storage.create_new_page()?;

        // Initialize the new page
        if let Ok(table_page) = self.tuple_storage.get_page(new_page_id) {
            table_page.write().as_page_trait_mut().set_dirty(true);

            if let Some(result) = try_update(new_page_id) {
                return result;
            }
        }

        Err("Failed to update tuple".to_string())
    }

    /// Updates the meta of a tuple.
    ///
    /// # Parameters
    ///
    /// - `meta`: New tuple meta.
    /// - `rid`: The RID of the inserted tuple.
    pub fn update_tuple_meta(&self, meta: &TupleMeta, rid: RID) -> Result<(), String> {
        let _write_guard = self.latch.write();

        let page_guard = self
            .tuple_storage
            .get_page(rid.get_page_id())
            .expect("Trying to update tuple_meta page");

        let mut table_page_guard = page_guard
            .into_specific_type::<TablePage, 8>()
            .ok_or_else(|| "Failed to convert to TablePage".to_string())?;

        let _ = table_page_guard
            .access_mut(|table_page| table_page.update_tuple_meta(meta, &rid))
            .expect("Failed to update table meta");

        Ok(())
    }

    pub fn get_bpm(&self) -> Arc<BufferPoolManager> {
        self.tuple_storage.bpm.clone()
    }

    /// Reads a tuple from the table.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the tuple to read.
    /// - `txn_ctx`: Optional transaction context for visibility checks
    ///
    /// # Returns
    ///
    /// A pair containing the meta and tuple.
    pub fn get_tuple(
        &self,
        rid: RID,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Result<(TupleMeta, Tuple), String> {
        let page_guard = self
            .page_manager
            .bpm
            .fetch_page_guarded(rid.get_page_id())
            .unwrap();

        let table_page_guard = page_guard
            .into_specific_type::<TablePage, 8>()
            .ok_or_else(|| "Failed to convert to TablePage".to_string())?;

        let (meta, tuple) = table_page_guard
            .access(|table_page| table_page.get_tuple(&rid).unwrap())
            .ok_or_else(|| "Failed to get tuple".to_string())?;

        // Check if tuple is deleted
        if meta.is_deleted() {
            return Err("Tuple is deleted".to_string());
        }

        // Check if tuple is visible to current transaction
        if let Some(txn_ctx) = &txn_ctx {
            let txn = txn_ctx.get_transaction();

            // Check if this is the creator transaction
            if meta.get_creator_txn_id() == txn.get_transaction_id() {
                return Ok((meta, tuple));
            }

            // Get the creator transaction
            let creator_txn_id = meta.get_creator_txn_id();
            let txn_manager = txn_ctx.get_transaction_manager();

            match txn.get_isolation_level() {
                IsolationLevel::ReadUncommitted => {
                    // Can see all tuples
                    Ok((meta, tuple))
                }
                IsolationLevel::ReadCommitted => {
                    // For READ_COMMITTED, we need to check if the creator transaction is committed
                    if creator_txn_id == INVALID_TXN_ID {
                        // If no creator transaction, tuple is visible
                        Ok((meta, tuple))
                    } else if let Some(creator_txn) = txn_manager.get_transaction(&creator_txn_id) {
                        if creator_txn.get_state() != TransactionState::Committed {
                            return Err("Tuple is not visible - creator transaction not committed".to_string());
                        }
                        Ok((meta, tuple))
                    } else {
                        // If creator transaction not found and it's not INVALID_TXN_ID,
                        // treat as uncommitted for safety
                        Err("Tuple is not visible - creator transaction not found".to_string())
                    }
                }
                IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                    // For REPEATABLE_READ and SERIALIZABLE, check commit timestamp
                    if creator_txn_id == INVALID_TXN_ID {
                        Ok((meta, tuple))
                    } else if let Some(creator_txn) = txn_manager.get_transaction(&creator_txn_id) {
                        if creator_txn.get_state() != TransactionState::Committed
                            || creator_txn.commit_ts() >= txn.read_ts() {
                            return Err("Tuple is not visible due to isolation level constraints".to_string());
                        }
                        Ok((meta, tuple))
                    } else {
                        // If creator transaction not found and it's not INVALID_TXN_ID,
                        // treat as uncommitted for safety
                        Err("Tuple is not visible - creator transaction not found".to_string())
                    }
                }
            }
        } else {
            // Without transaction context, just return the tuple if not deleted
            Ok((meta, tuple))
        }
    }

    /// Reads a tuple meta from the table. Note: if you want to get the tuple and meta together,
    /// use `get_tuple` instead to ensure atomicity.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the tuple to read.
    ///
    /// # Returns
    ///
    /// The tuple meta.
    pub fn get_tuple_meta(
        &self,
        rid: RID,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Result<TupleMeta, String> {
        let (meta, _) = self.get_tuple(rid, txn_ctx)?;
        Ok(meta)
    }

    /// Creates an iterator over the tuples in this table.
    ///
    /// # Parameters
    ///
    /// - `txn_ctx`: Optional transaction context for visibility checks
    ///
    /// # Returns
    ///
    /// A `TableIterator`.
    pub fn make_iterator(&self, txn_ctx: Option<Arc<TransactionContext>>) -> TableIterator {
        // Start from the first page that has tuples
        let first_page_id = self.get_first_page_id();
        debug!("Creating iterator starting from page_id: {}", first_page_id);

        // If no valid pages exist, create iterator with INVALID_PAGE_ID
        if first_page_id == INVALID_PAGE_ID {
            debug!("No valid pages found, creating empty iterator");
            return TableIterator::new(
                Arc::new(self.clone()),
                RID::new(INVALID_PAGE_ID, 0),
                RID::new(INVALID_PAGE_ID, 0),
                txn_ctx,
            );
        }

        // Create iterator starting from first tuple (first_page_id,0)
        let iterator = TableIterator::new(
            Arc::new(self.clone()),
            RID::new(first_page_id, 0),
            RID::new(INVALID_PAGE_ID, 0),
            txn_ctx,
        );

        debug!("Created iterator with RID: {:?}, is_end: {}", iterator.get_rid(), iterator.is_end());
        iterator
    }

    /// Returns an eager iterator of this table. The iterator will stop at the last tuple
    /// at the time of iterating.
    ///
    /// # Returns
    ///
    /// A `TableIterator`.
    pub fn make_eager_iterator(&self) -> TableIterator {
        // Implementation of make eager iterator logic here
        unimplemented!()
    }

    /// Gets the ID of the next page in the table.
    ///
    /// # Parameters
    /// - `current_page_id`: The ID of the current page
    ///
    /// # Returns
    /// The ID of the next page, or INVALID_PAGE_ID if there is no next page
    pub fn get_next_page_id(&self, current_page_id: PageId) -> PageId {
        if let Ok(page_guard) = self.tuple_storage.get_page(current_page_id) {
            if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                if let Some(next_page_id) = table_page.access(|page| page.get_next_page_id()) {
                    return next_page_id;
                }
            }
        }
        INVALID_PAGE_ID
    }

    /// Gets the first page ID (raw version without checking for tuples)
    fn get_first_page_id_raw(&self) -> PageId {
        *self.tuple_storage.first_page_id.read()
    }

    /// Gets the first page ID that contains tuples
    pub fn get_first_page_id(&self) -> PageId {
        // Get the first page that has tuples
        let mut current_page_id = self.get_first_page_id_raw();

        // If no pages exist yet, return INVALID_PAGE_ID
        if current_page_id == INVALID_PAGE_ID {
            return INVALID_PAGE_ID;
        }

        // Find first page with tuples
        while current_page_id != INVALID_PAGE_ID {
            if let Some(page_guard) = self.get_bpm().fetch_page_guarded(current_page_id) {
                if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                    if let Some(num_tuples) = table_page.access(|page| page.get_num_tuples()) {
                        if num_tuples > 0 {
                            return current_page_id;
                        }
                    }
                }
            }
            // Move to next page
            current_page_id = self.get_next_page_id(current_page_id);
        }

        // If no pages with tuples found, return INVALID_PAGE_ID
        INVALID_PAGE_ID
    }

    /// Acquires a read lock on a table page.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the page to lock.
    ///
    /// # Returns
    ///
    /// A `ReadPageGuard`.
    pub fn acquire_table_page_read_lock(&self, rid: RID) -> PageGuard {
        self.tuple_storage.get_page(rid.get_page_id()).unwrap()
    }

    /// Acquires a write lock on a table page.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the page to lock.
    ///
    /// # Returns
    ///
    /// A `WritePageGuard`.
    pub fn acquire_table_page_write_lock(&self, rid: RID) -> PageGuard {
        self.tuple_storage.get_page(rid.get_page_id()).unwrap()
    }

    /// Gets a tuple with the lock acquired.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the tuple.
    /// - `page`: The table page.
    ///
    /// # Returns
    ///
    /// A pair containing the meta and tuple.
    pub fn get_tuple_with_lock_acquired(&self, rid: RID, page: &TablePage) -> (TupleMeta, Tuple) {
        page.get_tuple(&rid).unwrap()
    }

    /// Gets a tuple meta with the lock acquired.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the tuple.
    /// - `page`: The table page.
    ///
    /// # Returns
    ///
    /// The tuple meta.
    pub fn get_tuple_meta_with_lock_acquired(&self, rid: RID, page: &TablePage) -> TupleMeta {
        page.get_tuple(&rid).unwrap().0
    }

    /// Helper method to get the number of pages
    pub fn get_num_pages(&self) -> usize {
        let mut count = 0;
        let mut current_page_id = *self.tuple_storage.first_page_id.read();
        while current_page_id != INVALID_PAGE_ID {
            count += 1;
            if let Some(page_guard) = self.tuple_storage.bpm.fetch_page_guarded(current_page_id) {
                if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                    table_page.access(|page| {
                        current_page_id = page.get_next_page_id();
                    });
                } else {
                    break;
                }
            } else {
                break;
            }
        }
        count
    }

    /// Helper method to get the total number of tuples
    pub fn get_num_tuples(&self) -> usize {
        let mut count = 0;
        let mut current_page_id = self.get_first_page_id();

        // If no pages exist yet, return 0
        if current_page_id == INVALID_PAGE_ID {
            return 0;
        }

        // Iterate through all pages and count tuples
        while current_page_id != INVALID_PAGE_ID {
            if let Some(page_guard) = self.get_bpm().fetch_page_guarded(current_page_id) {
                if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
                    if let Some(num_tuples) = table_page.access(|page| page.get_num_tuples()) {
                        count += num_tuples as usize;
                    }
                }
                // Get next page ID
                current_page_id = self.get_next_page_id(current_page_id);
            } else {
                break;
            }
        }
        count
    }

    /// Gets the table OID.
    pub fn get_table_oid(&self) -> TableOidT {
        self.tuple_storage.table_oid
    }
}

impl TableInfo {
    /// Constructs a new TableInfo instance.
    ///
    /// # Parameters
    /// - `schema`: The table schema.
    /// - `name`: The table name.
    /// - `table`: An owning pointer to the table heap.
    /// - `oid`: The unique OID for the table.
    pub fn new(
        schema: Schema,
        name: String,
        table: Arc<TableHeap>,
        oid: TableOidT,
    ) -> Self {
        TableInfo {
            schema,
            name,
            table,
            oid,
        }
    }

    pub fn get_table_schema(&self) -> Schema {
        self.schema.clone()
    }

    pub fn get_table_oidt(&self) -> TableOidT {
        self.oid
    }

    /// Gets a mutable reference to the table heap
    pub fn get_table_heap_mut(&self) -> Arc<TableHeap> {
        self.table.clone()
    }

    /// Gets an immutable reference to the table heap
    pub fn get_table_heap(&self) -> Arc<TableHeap> {
        self.table.clone()
    }

    pub fn get_table_name(&self) -> &str {
        &self.name
    }
}

impl TupleStorage {
    pub fn new(bpm: Arc<BufferPoolManager>, table_oid: TableOidT) -> Self {
        Self {
            bpm,
            first_page_id: RwLock::new(INVALID_PAGE_ID),
            last_page_id: RwLock::new(INVALID_PAGE_ID),
            table_oid,
        }
    }

    pub fn insert_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Result<RID, String> {
        // Acquire write lock at the beginning of the method
        let _write_guard = RwLock::new(());
        let _guard = _write_guard.write();

        // Handle transaction context first
        if let Some(txn_ctx) = &txn_ctx {
            let txn = txn_ctx.get_transaction().clone();

            // Acquire table-level lock for insert
            let lock_manager = txn_ctx.get_lock_manager();
            if let Err(e) = lock_manager.lock_table(txn, LockMode::Exclusive, self.table_oid) {
                return Err(format!("Failed to acquire table lock: {}", e));
            }
        }

        // Check if we need to create the first page
        let first_page_id = *self.first_page_id.read();
        if first_page_id == INVALID_PAGE_ID {
            // Create and initialize the first page
            if let Some(new_page) = self.bpm.new_page_guarded(NewPageType::Table) {
                let new_page_id = new_page.get_page_id();

                // Initialize the new page
                if let Some(mut table_page) = new_page.into_specific_type::<TablePage, 8>() {
                    table_page.access_mut(|page| {
                        page.init();
                        page.set_dirty(true);
                    });
                }

                // Update first and last page pointers
                *self.first_page_id.write() = new_page_id;
                *self.last_page_id.write() = new_page_id;
            } else {
                return Err("Failed to create first page".to_string());
            }
        }

        // Try inserting into the last page first
        let last_page_id = *self.last_page_id.read();
        if let Ok(rid) = self.try_insert_in_page(last_page_id, meta, tuple, &txn_ctx) {
            return Ok(rid);
        }

        // If that fails, create a new page and try again
        let new_page_id = self.create_new_page()?;
        self.try_insert_in_page(new_page_id, meta, tuple, &txn_ctx)
    }

    fn try_insert_in_page(
        &self,
        page_id: PageId,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        txn_ctx: &Option<Arc<TransactionContext>>,
    ) -> Result<RID, String> {
        let page_guard = self.get_page(page_id)?;

        if let Some(mut table_page) = page_guard.into_specific_type::<TablePage, 8>() {
            // Let TablePage handle the physical insertion
            let rid = table_page.access_mut(|page| {
                // Check space at page level using get_length instead of get_size
                if let Ok(tuple_size) = tuple.get_length() {
                    if !page.has_space_for(tuple_size) {
                        return None;
                    }

                    // Delegate actual insertion to TablePage
                    page.insert_tuple(meta, tuple)
                } else {
                    None
                }
            });

            // Handle transaction tracking if insertion was successful
            if let Some(Some(rid)) = rid {
                if let Some(txn_ctx) = txn_ctx {
                    txn_ctx.append_write_set_atomic(self.table_oid, rid);
                }
                return Ok(rid);
            }
        }

        Err("Failed to insert tuple into page".to_string())
    }

    pub fn update_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        rid: RID,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Result<RID, String> {
        let page_guard = self.get_page(rid.get_page_id())?;

        if let Some(mut table_page) = page_guard.into_specific_type::<TablePage, 8>() {
            let result = table_page.access_mut(|page| {
                match page.update_tuple(meta, tuple, rid) {
                    Ok(()) => {
                        // Update successful in place
                        if let Some(txn_ctx) = &txn_ctx {
                            txn_ctx.append_write_set_atomic(self.table_oid, rid);
                        }
                        Ok(rid)
                    }
                    Err(_) => {
                        // Try inserting as new tuple if update fails
                        match page.insert_tuple(meta, tuple) {
                            Some(new_rid) => {
                                if let Some(txn_ctx) = &txn_ctx {
                                    txn_ctx.append_write_set_atomic(self.table_oid, new_rid);
                                }
                                Ok(new_rid)
                            }
                            None => Err("Failed to update/insert tuple".to_string()),
                        }
                    }
                }
            });

            if let Some(result) = result {
                return result;
            }
        }

        Err("Failed to update tuple".to_string())
    }

    pub fn get_tuple(&self, rid: RID) -> Result<(TupleMeta, Tuple), String> {
        let page_guard = self.get_page(rid.get_page_id())?;

        if let Some(table_page) = page_guard.into_specific_type::<TablePage, 8>() {
            if let Some(result) = table_page.access(|page| page.get_tuple(&rid)) {
                return Ok(result.unwrap());
            }
        }

        Err("Failed to get tuple".to_string())
    }

    fn create_new_page(&self) -> Result<PageId, String> {
        // Create new page
        let new_page = self
            .bpm
            .new_page_guarded(NewPageType::Table)
            .ok_or_else(|| "Failed to create new page".to_string())?;
        let new_page_id = new_page.get_page_id();

        // Initialize the new page
        if let Some(mut table_page) = new_page.into_specific_type::<TablePage, 8>() {
            table_page.access_mut(|page| {
                page.init();
                page.set_dirty(true);
            });

            // Update the page links
            let last_page_id = *self.last_page_id.read();
            if let Ok(last_page) = self.get_page(last_page_id) {
                if let Some(mut last_table_page) = last_page.into_specific_type::<TablePage, 8>() {
                    last_table_page.access_mut(|page| {
                        page.set_next_page_id(new_page_id);
                        page.set_dirty(true);
                    });
                }
            }

            // Update last page pointer
            *self.last_page_id.write() = new_page_id;

            Ok(new_page_id)
        } else {
            Err("Failed to initialize new page".to_string())
        }
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageGuard, String> {
        self.bpm
            .fetch_page_guarded(page_id)
            .ok_or_else(|| "Failed to fetch page".to_string())
    }

    pub fn get_first_page_id(&self) -> PageId {
        *self.first_page_id.read()
    }

    pub fn get_last_page_id(&self) -> PageId {
        *self.last_page_id.read()
    }

    pub fn get_table_oid(&self) -> TableOidT {
        self.table_oid
    }
}

impl Debug for TableHeap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableHeap")
            .field("first_page_id", &self.tuple_storage.first_page_id.read())
            .field("last_page_id", &self.tuple_storage.last_page_id.read())
            .field("bpm", &"Arc<BufferPoolManager>") // We don't debug the BPM itself
            .field("num_pages", &self.get_num_pages())
            .field("num_tuples", &self.get_num_tuples())
            .finish()
    }
}

impl PartialEq for TableInfo {
    fn eq(&self, other: &Self) -> bool {
        // Compare schema
        self.schema == other.schema &&
            // Compare table name
            self.name == other.name &&
            // Compare table OID
            self.oid == other.oid &&
            // Compare table heap Arc by comparing the internal pointer
            Arc::ptr_eq(&self.table, &other.table)
    }
}

impl Clone for TablePageManager {
    fn clone(&self) -> Self {
        Self {
            bpm: self.bpm.clone(),
            first_page_id: self.first_page_id,
            last_page_id: self.last_page_id,
        }
    }
}

impl Clone for TupleStorage {
    fn clone(&self) -> Self {
        Self {
            bpm: self.bpm.clone(),
            first_page_id: RwLock::new(*self.first_page_id.read()),
            last_page_id: RwLock::new(*self.last_page_id.read()),
            table_oid: self.table_oid,
        }
    }
}

impl Clone for TableTransactionManager {
    fn clone(&self) -> Self {
        Self {
            lock_manager: self.lock_manager.clone(),
            transaction_manager: self.transaction_manager.clone(),
            table_oid: self.table_oid,
        }
    }
}

impl Clone for TableHeap {
    fn clone(&self) -> Self {
        Self {
            page_manager: self.page_manager.clone(),
            tuple_storage: self.tuple_storage.clone(),
            txn_manager: self.txn_manager.clone(),
            latch: RwLock::new(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::common::time::TimeStamp;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;
    use std::time::{SystemTime, UNIX_EPOCH};
    use tempfile::TempDir;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        txn_manager: Arc<TransactionManager>,

        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(name: &str) -> Self {
            initialize_logger();
            let buffer_pool_size: usize = 5;
            const K: usize = 2;
            // Create temporary directory
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

            // Create disk components
            let disk_manager = Arc::new(FileDiskManager::new(db_path, log_path, 10));
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));

            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                buffer_pool_size,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));

            // Create log manager and transaction manager
            let txn_manager = Arc::new(TransactionManager::new());

            Self {
                bpm,
                txn_manager,
                _temp_dir: temp_dir,
            }
        }
    }

    fn setup_test_table(ctx: &TestContext) -> TableHeap {
        TableHeap::new(
            ctx.bpm.clone(),
            1, // table_oid
            ctx.txn_manager.clone(),
        )
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    #[test]
    fn test_table_heap_creation() {
        let ctx = TestContext::new("test_table_heap_creation");
        let table_oid = 0;
        let table_heap = TableHeap::new(ctx.bpm.clone(), table_oid, ctx.txn_manager.clone());

        // The first page ID should be invalid since no pages have been created yet
        assert_eq!(table_heap.get_first_page_id(), INVALID_PAGE_ID);
        // A new table heap starts with 0 pages
        assert_eq!(table_heap.get_num_pages(), 0);
        // And 0 tuples
        assert_eq!(table_heap.get_num_tuples(), 0);
    }

    #[test]
    fn test_insert_and_get_tuple() {
        let ctx = TestContext::new("test_insert_and_get_tuple");
        let table_heap = setup_test_table(&ctx);

        let lock_manager = Arc::new(LockManager::new());

        // Create and initialize first transaction through the transaction manager
        let txn = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        txn.set_state(TransactionState::Growing);
        let txn_ctx = Arc::new(TransactionContext::new(
            txn.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));


        let schema = create_test_schema();
        let tuple_values = vec![Value::new(1), Value::new("Alice"), Value::new(30)];
        let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0));
        let mut meta = TupleMeta::new(0);

        // Insert tuple
        let rid = table_heap
            .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
            .expect("Failed to insert tuple");

        // Set commit timestamp and commit the transaction
        let commit_ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        meta.set_commit_timestamp(TimeStamp::new(commit_ts));
        txn.set_state(TransactionState::Committed);

        table_heap
            .update_tuple_meta(&meta, rid)
            .expect("Failed to update tuple metadata");

        // Create a new transaction for reading
        let read_txn = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
        read_txn.set_state(TransactionState::Growing);
        let read_txn_ctx = Arc::new(TransactionContext::new(
            read_txn.clone(),
            lock_manager,
            ctx.txn_manager.clone(),
        ));

        // Read the tuple with the new transaction
        let (retrieved_meta, retrieved_tuple) = table_heap
            .get_tuple(rid, Some(read_txn_ctx.clone()))
            .expect("Failed to get tuple");

        assert_eq!(retrieved_meta, meta);
        assert_eq!(retrieved_tuple, tuple);
        assert_eq!(table_heap.get_num_tuples(), 1);

        // Clean up
        read_txn.set_state(TransactionState::Committed);
    }

    #[test]
    fn test_update_tuple_meta() {
        let ctx = TestContext::new("test_update_tuple_meta");
        let bpm = ctx.bpm.clone();
        let table_oid = 1;
        let table_heap = TableHeap::new(bpm, table_oid, ctx.txn_manager.clone());
        let schema = create_test_schema();

        let tuple_values = vec![Value::new(1), Value::new("Bob"), Value::new(25)];
        let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0));
        let meta = TupleMeta::new(0);

        let rid = table_heap
            .insert_tuple(&meta, &mut tuple, None)
            .expect("Failed to insert tuple");

        let updated_meta = TupleMeta::new(1);
        table_heap
            .update_tuple_meta(&updated_meta, rid)
            .expect("Failed to update tuple meta");

        let retrieved_meta = table_heap
            .get_tuple_meta(rid, None)
            .expect("Failed to get tuple meta");

        assert_eq!(retrieved_meta, updated_meta);
    }

    #[test]
    fn test_table_iterator() {
        let ctx = TestContext::new("test_table_iterator");
        let lock_manager = Arc::new(LockManager::new());

        // Create and initialize first transaction through the transaction manager
        let txn = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        txn.set_state(TransactionState::Growing);
        let txn_ctx = Arc::new(TransactionContext::new(
            txn.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        let table_heap = setup_test_table(&ctx);
        debug!("Initial table heap state: {:?}", table_heap);

        let schema = Schema::new(vec![
            Column::new("col_1", TypeId::Integer),
            Column::new("col_2", TypeId::VarChar),
            Column::new("col_3", TypeId::Integer),
        ]);

        // Insert multiple tuples
        let mut inserted_rids = Vec::new();
        let mut metas = Vec::new();
        for i in 0..5 {
            let tuple_values = vec![
                Value::new(i),
                Value::new(format!("Name{}", i)),
                Value::new(20 + i),
            ];
            let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, i as u32));
            let mut meta = TupleMeta::new(txn.get_transaction_id());

            // Set commit timestamp before inserting
            let commit_ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as u64;
            meta.set_commit_timestamp(TimeStamp::new(commit_ts));

            let rid = table_heap
                .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
                .expect("Failed to insert tuple");

            debug!("Inserted tuple with RID: {:?}", rid);

            // Update tuple metadata to mark it as committed
            table_heap
                .update_tuple_meta(&meta, rid)
                .expect("Failed to update tuple metadata");

            // Verify tuple was inserted correctly
            let (stored_meta, stored_tuple) = table_heap
                .get_tuple(rid, Some(txn_ctx.clone()))
                .expect("Failed to get inserted tuple");
            debug!(
                "Verified tuple - RID: {:?}, Meta: {:?}, Tuple: {:?}",
                rid, stored_meta, stored_tuple
            );

            inserted_rids.push(rid);
            metas.push((rid, meta));
        }

        debug!("Table heap after insertions: {:?}", table_heap);
        debug!("First page ID: {}", table_heap.get_first_page_id());
        debug!("Number of tuples: {}", table_heap.get_num_tuples());

        // Create iterator before committing transaction
        let iterator = table_heap.make_iterator(Some(txn_ctx.clone()));
        debug!("Created iterator: {:?}", iterator);

        // Collect tuples while transaction is still in Growing state
        let tuples = iterator.collect::<Vec<(TupleMeta, Tuple)>>();
        debug!("Collected {} tuples", tuples.len());

        // Now we can commit the transaction
        txn.set_state(TransactionState::Committed);
        txn.set_commit_ts(SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64);

        for (i, (meta, tuple)) in tuples.iter().enumerate() {
            debug!("Retrieved tuple {}: Meta: {:?}, Tuple: {:?}", i, meta, tuple);
        }

        assert_eq!(tuples.len(), 5, "Expected 5 tuples, but got {}", tuples.len());

        // Verify tuples and metadata
        for (i, ((_rid, meta), (retrieved_meta, retrieved_tuple))) in
            metas.iter().zip(tuples.iter()).enumerate()
        {
            assert_eq!(retrieved_meta, meta, "Tuple {} metadata should match", i);
            assert_eq!(retrieved_tuple.get_value(0), &Value::new(i as i32));
            assert_eq!(
                retrieved_tuple.get_value(1),
                &Value::new(format!("Name{}", i))
            );
            assert_eq!(retrieved_tuple.get_value(2), &Value::new(20 + i as i32));
        }
    }

    #[test]
    fn test_table_heap_debug() {
        let ctx = TestContext::new("test_table_heap_debug");
        let bpm = ctx.bpm.clone();
        let table_oid = 1;
        let table_heap = TableHeap::new(bpm, table_oid, ctx.txn_manager.clone());

        let debug_output = format!("{:?}", table_heap);
        assert!(debug_output.contains("TableHeap"));
        assert!(debug_output.contains("first_page_id"));
        assert!(debug_output.contains("last_page_id"));
        assert!(debug_output.contains("num_pages"));
        assert!(debug_output.contains("num_tuples"));
    }

    #[test]
    fn test_concurrent_transactions() {
        let ctx = TestContext::new("test_concurrent_transactions");
        let table_heap = setup_test_table(&ctx);
        let schema = create_test_schema();

        let lock_manager = Arc::new(LockManager::new());

        // Create and initialize first transaction through the transaction manager
        let txn1 = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        txn1.set_state(TransactionState::Growing);
        let txn_ctx1 = Arc::new(TransactionContext::new(
            txn1.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Insert tuple with transaction 1
        let tuple_values1 = vec![Value::new(1), Value::new("Alice"), Value::new(25)];
        let mut tuple1 = Tuple::new(&tuple_values1, schema.clone(), RID::new(0, 0));
        let meta1 = TupleMeta::new(txn1.get_transaction_id());

        let rid1 = table_heap
            .insert_tuple(&meta1, &mut tuple1, Some(txn_ctx1.clone()))
            .expect("Failed to insert tuple with txn1");

        // Create second transaction through the transaction manager
        let txn2 = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        txn2.set_state(TransactionState::Growing);
        let txn_ctx2 = Arc::new(TransactionContext::new(
            txn2.clone(),
            lock_manager,
            ctx.txn_manager.clone(),
        ));

        // Try to read the uncommitted tuple with transaction 2
        let result = table_heap.get_tuple(rid1, Some(txn_ctx2.clone()));
        assert!(
            result.is_err(),
            "Transaction 2 should not see uncommitted tuple"
        );
        assert_eq!(
            result.unwrap_err(),
            "Tuple is not visible - creator transaction not committed".to_string()
        );

        // Now commit transaction 1
        txn1.set_state(TransactionState::Committed);
        let commit_ts1 = txn1.commit_ts();
        let mut committed_meta1 = meta1;
        committed_meta1.set_commit_timestamp(TimeStamp::new(commit_ts1));
        table_heap
            .update_tuple_meta(&committed_meta1, rid1)
            .expect("Failed to update tuple metadata");

        // Now transaction 2 should be able to read the tuple
        let result = table_heap.get_tuple(rid1, Some(txn_ctx2.clone()));
        assert!(result.is_ok(), "Transaction 2 should see committed tuple");

        // Clean up
        txn2.set_state(TransactionState::Committed);
    }

    #[test]
    fn test_write_write_conflict() {
        let ctx = TestContext::new("test_write_write_conflict");
        let table_heap = setup_test_table(&ctx);
        let schema = create_test_schema();

        let lock_manager = Arc::new(LockManager::new());

        // Create and initialize first transaction through the transaction manager
        let txn1 = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        txn1.set_state(TransactionState::Growing);
        let txn_ctx1 = Arc::new(TransactionContext::new(
            txn1.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Insert initial tuple with transaction 1
        let tuple_values = vec![Value::new(1), Value::new("Alice"), Value::new(25)];
        let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0));
        let meta1 = TupleMeta::new(txn1.get_transaction_id());

        // Insert and immediately commit transaction 1
        let rid = table_heap
            .insert_tuple(&meta1, &mut tuple, Some(txn_ctx1.clone()))
            .expect("Failed to insert tuple with txn1");

        // Commit transaction 1 and update tuple metadata
        let commit_txn1 = ctx.txn_manager.commit(txn1.clone(), ctx.bpm.clone());
        let commit_ts1 = txn1.commit_ts();
        let mut committed_meta1 = meta1;
        committed_meta1.set_commit_timestamp(TimeStamp::new(commit_ts1));
        txn1.set_state(TransactionState::Committed);
        table_heap
            .update_tuple_meta(&committed_meta1, rid)
            .expect("Failed to update tuple metadata");

        // Create second transaction through the transaction manager
        let txn2 = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        txn2.set_state(TransactionState::Growing);
        let txn_ctx2 = Arc::new(TransactionContext::new(
            txn2.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Create third transaction with REPEATABLE_READ before committing txn1
        let txn3 = ctx.txn_manager
            .begin(IsolationLevel::RepeatableRead)
            .unwrap();
        txn3.set_state(TransactionState::Running);
        // Set read timestamp to be after txn1's commit
        txn3.set_read_ts(commit_ts1 + 1);
        let txn_ctx3 = Arc::new(TransactionContext::new(
            txn3.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Start update with transaction 2
        let new_values2 = vec![Value::new(1), Value::new("Bob"), Value::new(30)];
        let mut new_tuple2 = Tuple::new(&new_values2, schema.clone(), rid);
        let meta2 = TupleMeta::new(txn2.get_transaction_id());

        // Successfully acquire lock and start update with transaction 2
        let result2 = table_heap.update_tuple(&meta2, &mut new_tuple2, rid, Some(txn_ctx2.clone()));
        assert!(result2.is_ok(), "First update should succeed");

        // Try concurrent update with transaction 3 - should fail
        let new_values3 = vec![Value::new(1), Value::new("Charlie"), Value::new(35)];
        let mut new_tuple3 = Tuple::new(&new_values3, schema.clone(), rid);
        let meta3 = TupleMeta::new(txn3.get_transaction_id());

        let result3 = table_heap.update_tuple(&meta3, &mut new_tuple3, rid, Some(txn_ctx3.clone()));
        assert!(
            result3.is_err(),
            "Concurrent update should fail due to write-write conflict"
        );

        // Clean up
        let commit_txn2 = ctx.txn_manager.commit(txn2.clone(), ctx.bpm.clone());
        let commit_txn3 = ctx.txn_manager.commit(txn3.clone(), ctx.bpm);
    }

    #[test]
    fn test_large_tuple_handling() {
        let ctx = TestContext::new("test_large_tuple_handling");
        let table_heap = setup_test_table(&ctx);

        // Create a schema with a large varchar column
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("large_text", TypeId::VarChar),
        ]);

        // Create a very large tuple that should span multiple pages
        let large_string = "x".repeat(10000); // 10KB string
        let tuple_values = vec![Value::new(1), Value::new(large_string)];
        let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0));
        let meta = TupleMeta::new(0);

        // Attempt to insert the large tuple
        let result = table_heap.insert_tuple(&meta, &mut tuple, None);
        assert!(
            result.is_err(),
            "Should fail to insert tuple larger than page size"
        );
    }

    #[test]
    fn test_delete_and_reinsert() {
        let ctx = TestContext::new("test_delete_and_reinsert");
        let table_heap = setup_test_table(&ctx);
        let schema = create_test_schema();

        // Create transaction
        let txn = Arc::new(Transaction::new(1, IsolationLevel::ReadCommitted));
        txn.set_state(TransactionState::Growing);
        let lock_manager = Arc::new(LockManager::new());
        let txn_ctx = Arc::new(TransactionContext::new(
            txn.clone(),
            lock_manager,
            ctx.txn_manager.clone(),
        ));

        // Insert initial tuple
        let tuple_values = vec![Value::new(1), Value::new("Alice"), Value::new(25)];
        let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0));
        let mut meta = TupleMeta::new(1);

        let rid = table_heap
            .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
            .expect("Failed to insert tuple");

        // Mark tuple as deleted
        meta.set_deleted(true);
        table_heap
            .update_tuple_meta(&meta, rid)
            .expect("Failed to mark tuple as deleted");

        // Verify tuple is not visible
        let result = table_heap.get_tuple(rid, Some(txn_ctx.clone()));
        assert!(result.is_err(), "Deleted tuple should not be visible");

        // Try to insert a new tuple - should reuse the space
        let new_values = vec![Value::new(2), Value::new("Bob"), Value::new(30)];
        let mut new_tuple = Tuple::new(&new_values, schema.clone(), RID::new(0, 0));
        let new_meta = TupleMeta::new(1);

        let new_rid = table_heap
            .insert_tuple(&new_meta, &mut new_tuple, Some(txn_ctx.clone()))
            .expect("Failed to insert new tuple");

        // Verify the new tuple is visible
        let result = table_heap.get_tuple(new_rid, Some(txn_ctx.clone()));
        assert!(result.is_ok(), "New tuple should be visible");
    }

    #[test]
    fn test_update_with_different_sizes() {
        let ctx = TestContext::new("test_update_with_different_sizes");
        let table_heap = setup_test_table(&ctx);
        let schema = create_test_schema();
        let lock_manager = Arc::new(LockManager::new());

        // Create transaction
        let txn = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        let txn_ctx = Arc::new(TransactionContext::new(
            txn.clone(),
            lock_manager,
            ctx.txn_manager.clone(),
        ));

        // Insert initial tuple
        let tuple_values = vec![Value::new(1), Value::new("short"), Value::new(25)];
        let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0));
        let meta = TupleMeta::new(txn.get_transaction_id());

        let rid = table_heap
            .insert_tuple(&meta, &mut tuple, Some(txn_ctx.clone()))
            .expect("Failed to insert initial tuple");

        // Update with a larger value
        let larger_values = vec![
            Value::new(1),
            Value::new("this is a much longer string"),
            Value::new(25),
        ];
        let mut larger_tuple = Tuple::new(&larger_values, schema.clone(), rid);
        let result = table_heap.update_tuple(&meta, &mut larger_tuple, rid, Some(txn_ctx.clone()));
        assert!(result.is_ok(), "Update with larger value should succeed");

        // Update with a smaller value
        let smaller_values = vec![Value::new(1), Value::new("tiny"), Value::new(25)];
        let mut smaller_tuple = Tuple::new(&smaller_values, schema.clone(), rid);
        let result = table_heap.update_tuple(&meta, &mut smaller_tuple, rid, Some(txn_ctx.clone()));
        assert!(result.is_ok(), "Update with smaller value should succeed");

        // Verify final state
        let (_, final_tuple) = table_heap
            .get_tuple(rid, Some(txn_ctx.clone()))
            .expect("Failed to get final tuple");
        assert_eq!(
            final_tuple.get_value(1),
            &Value::new("tiny"),
            "Final tuple should contain the last update"
        );
    }

    #[test]
    fn test_update_with_isolation_levels() {
        let ctx = TestContext::new("test_update_with_isolation_levels");
        let table_heap = setup_test_table(&ctx);
        let schema = create_test_schema();
        let lock_manager = Arc::new(LockManager::new());

        // Create first transaction with REPEATABLE_READ
        let txn1 = ctx.txn_manager
            .begin(IsolationLevel::RepeatableRead)
            .unwrap();
        txn1.set_state(TransactionState::Running);
        let txn_ctx1 = Arc::new(TransactionContext::new(
            txn1.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Insert initial tuple
        let tuple_values = vec![Value::new(1), Value::new("initial"), Value::new(25)];
        let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0));
        let mut meta1 = TupleMeta::new(txn1.get_transaction_id());

        let rid = table_heap
            .insert_tuple(&meta1, &mut tuple, Some(txn_ctx1.clone()))
            .expect("Failed to insert tuple");

        // Commit first transaction
        let commit_txn1 = ctx.txn_manager.commit(txn1.clone(), ctx.bpm.clone());

        // Get commit timestamp from txn1 and update tuple metadata
        let commit_ts1 = txn1.commit_ts();
        meta1.set_commit_timestamp(TimeStamp::new(commit_ts1));
        table_heap
            .update_tuple_meta(&meta1, rid)
            .expect("Failed to update tuple metadata");

        // Create third transaction with REPEATABLE_READ after txn1's commit
        let txn3 = ctx.txn_manager
            .begin(IsolationLevel::RepeatableRead)
            .unwrap();
        txn3.set_state(TransactionState::Running);
        // Set read timestamp to be after txn1's commit
        txn3.set_read_ts(commit_ts1 + 1);
        let txn_ctx3 = Arc::new(TransactionContext::new(
            txn3.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Create second transaction with READ_COMMITTED
        let txn2 = ctx.txn_manager
            .begin(IsolationLevel::ReadCommitted)
            .unwrap();
        txn2.set_state(TransactionState::Running);
        let txn_ctx2 = Arc::new(TransactionContext::new(
            txn2.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Update tuple with second transaction
        let new_values = vec![Value::new(1), Value::new("updated"), Value::new(26)];
        let mut new_tuple = Tuple::new(&new_values, schema.clone(), rid);
        let mut meta2 = TupleMeta::new(txn2.get_transaction_id());

        let result = table_heap.update_tuple(&meta2, &mut new_tuple, rid, Some(txn_ctx2.clone()));
        assert!(result.is_ok(), "Update with READ_COMMITTED should succeed");

        // Commit second transaction and update its metadata
        let commit_txn2 = ctx.txn_manager.commit(txn2.clone(), ctx.bpm.clone());
        let commit_ts2 = txn2.commit_ts();
        meta2.set_commit_timestamp(TimeStamp::new(commit_ts2));
        table_heap
            .update_tuple_meta(&meta2, result.unwrap())
            .expect("Failed to update tuple metadata");

        // Try to read tuple with REPEATABLE_READ transaction (txn3)
        // txn3 should see the original version since it started before txn2's update
        let result = table_heap.get_tuple(rid, Some(txn_ctx3.clone()));
        assert!(
            result.is_ok(),
            "REPEATABLE_READ transaction should see original version"
        );
        let (_, read_tuple) = result.unwrap();
        assert_eq!(
            read_tuple.get_value(1),
            &Value::new("initial"),
            "REPEATABLE_READ transaction should see original value"
        );

        // Clean up
        let commit_txn3 = ctx.txn_manager.commit(txn3.clone(), ctx.bpm);
    }

    #[test]
    fn test_concurrent_update_same_tuple() {
        let ctx = TestContext::new("test_concurrent_update_same_tuple");
        let table_heap = setup_test_table(&ctx);
        let schema = create_test_schema();
        let lock_manager = Arc::new(LockManager::new());

        // Create initial transaction
        let txn1 = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        let txn_ctx1 = Arc::new(TransactionContext::new(
            txn1.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        // Insert initial tuple
        let tuple_values = vec![Value::new(1), Value::new("initial"), Value::new(25)];
        let mut tuple = Tuple::new(&tuple_values, schema.clone(), RID::new(0, 0));
        let meta1 = TupleMeta::new(txn1.get_transaction_id());

        let rid = table_heap
            .insert_tuple(&meta1, &mut tuple, Some(txn_ctx1.clone()))
            .expect("Failed to insert tuple");

        // Create two concurrent transactions
        let txn2 = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        let txn_ctx2 = Arc::new(TransactionContext::new(
            txn2.clone(),
            lock_manager.clone(),
            ctx.txn_manager.clone(),
        ));

        let txn3 = ctx.txn_manager.begin(IsolationLevel::ReadCommitted).unwrap();
        let txn_ctx3 = Arc::new(TransactionContext::new(
            txn3.clone(),
            lock_manager,
            ctx.txn_manager.clone(),
        ));

        // First update should succeed
        let values2 = vec![Value::new(1), Value::new("update2"), Value::new(26)];
        let mut tuple2 = Tuple::new(&values2, schema.clone(), rid);
        let meta2 = TupleMeta::new(txn2.get_transaction_id());
        let result2 = table_heap.update_tuple(&meta2, &mut tuple2, rid, Some(txn_ctx2.clone()));
        assert!(result2.is_ok(), "First concurrent update should succeed");

        // Second update should fail due to write-write conflict
        let values3 = vec![Value::new(1), Value::new("update3"), Value::new(27)];
        let mut tuple3 = Tuple::new(&values3, schema.clone(), rid);
        let meta3 = TupleMeta::new(txn3.get_transaction_id());
        let result3 = table_heap.update_tuple(&meta3, &mut tuple3, rid, Some(txn_ctx3.clone()));
        assert!(
            result3.is_err(),
            "Second concurrent update should fail due to write-write conflict"
        );
        assert!(
            result3.unwrap_err().contains("Write-write conflict"),
            "Error should indicate write-write conflict"
        );
    }
}
