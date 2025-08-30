use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
use crate::catalog::schema::Schema;
use crate::common::config::{PageId, TableOidT, INVALID_PAGE_ID};
use crate::common::exception::PageError;
use crate::common::rid::RID;
use crate::concurrency::lock_manager::LockManager;
use crate::concurrency::transaction::IsolationLevel;
use crate::concurrency::transaction::UndoLink;
use crate::concurrency::transaction::UndoLog;
use crate::concurrency::transaction_manager::TransactionManager;
use crate::sql::execution::transaction_context::TransactionContext;
use crate::storage::page::page::{Page, PageTrait};
use crate::storage::page::page_guard::PageGuard;
use crate::storage::page::page_types::table_page::TablePage;
use crate::storage::table::table_iterator::TableIterator;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use crate::types_db::value::Value;
use log::debug;
use parking_lot::RwLock;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

// Page Management
pub struct TablePageManager {
    bpm: Arc<BufferPoolManager>,
    first_page_id: RwLock<PageId>,
    last_page_id: RwLock<PageId>,
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

/// TableHeap represents a physical table on a disk.
/// This is just a doubly linked list of pages.
pub struct TableHeap {
    bpm: Arc<BufferPoolManager>,
    first_page_id: RwLock<PageId>,
    last_page_id: RwLock<PageId>,
    table_oid: TableOidT,
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

impl TablePageManager {
    pub fn new(bpm: Arc<BufferPoolManager>) -> Self {
        Self {
            bpm,
            first_page_id: RwLock::new(INVALID_PAGE_ID),
            last_page_id: RwLock::new(INVALID_PAGE_ID),
        }
    }
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
    pub fn new(bpm: Arc<BufferPoolManager>, table_oid: TableOidT) -> Self {
        let heap = Self {
            bpm: bpm.clone(),
            first_page_id: RwLock::new(INVALID_PAGE_ID),
            last_page_id: RwLock::new(INVALID_PAGE_ID),
            table_oid,
            latch: RwLock::new(()),
        };

        // Try to create the first page - if this fails, the table will be empty
        // and the first insert will create the initial page
        if let Some(page_guard) = bpm.new_page::<TablePage>() {
            let first_page_id = page_guard.read().get_page_id();

            // Initialize the page
            {
                let mut page = page_guard.write();
                page.init();
                page.set_dirty(true);
            }

            // Update page IDs
            *heap.first_page_id.write() = first_page_id;
            *heap.last_page_id.write() = first_page_id;
        }

        heap
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
    pub fn insert_tuple(&self, meta: Arc<TupleMeta>, tuple: &Tuple) -> Result<RID, String> {
        let _write_guard = self.latch.write();

        // Check if we need to create the first page
        let last_page_id = *self.last_page_id.read();
        if last_page_id == INVALID_PAGE_ID {
            // Table is empty, create the first page
            return self.create_first_page_and_insert(&meta, tuple);
        }

        // Get the last page
        let page_guard = self.get_page(last_page_id)?;

        // Try to insert into the current page
        {
            let mut page = page_guard.write();
            if page.has_space_for(tuple) {
                return match page.insert_tuple(&meta, tuple) {
                    Some(rid) => {
                        page.set_dirty(true);
                        Ok(rid)
                    }
                    None => {
                        Err(
                            "Failed to insert tuple into page despite having space".to_string()
                        )
                    }
                }
            }
        }

        // If we get here, we need a new page
        // Note: page_guard is dropped here before creating a new page
        self.create_new_page_and_insert(&meta, tuple)
    }

    pub fn update_tuple(
        &self,
        meta: Arc<TupleMeta>,
        tuple: &Tuple,
        rid: RID,
        txn_ctx: Option<Arc<TransactionContext>>,
    ) -> Result<RID, String> {
        let _write_guard = self.latch.write();

        // First, get the current tuple to check visibility
        let page_guard = self.get_page(rid.get_page_id())?;
        let page = page_guard.read();

        // Get the current version for visibility check
        let (current_meta, current_tuple) = page.get_tuple(&rid, false)?;

        // Check if the tuple is deleted
        if current_meta.is_deleted() {
            return Err("Cannot update deleted tuple".to_string());
        }

        // Check visibility rules:
        // 1. If the current transaction created the tuple, allow update
        // 2. If the tuple is committed, allow update
        // 3. If the tuple is uncommitted and created by another transaction, deny update
        if current_meta.get_creator_txn_id() != meta.get_creator_txn_id()
            && !current_meta.is_committed()
        {
            return Err("Cannot update uncommitted tuple from another transaction".to_string());
        }

        // Drop the read lock before acquiring write lock
        drop(page);

        // Create undo log entry before update if we have a transaction context
        if let Some(txn_ctx) = txn_ctx {
            let txn = txn_ctx.get_transaction();
            let txn_manager = txn_ctx.get_transaction_manager();

            // Always create the undo log for any modification
            let undo_log = UndoLog::new(
                false,
                vec![true; tuple.get_column_count()],
                Arc::from(current_tuple), // Keep the old version
                current_meta.get_commit_timestamp(),
                UndoLink::new(
                    current_meta.get_creator_txn_id(),
                    current_meta.get_undo_log_idx(),
                ),
            );

            // Append the undo log to the transaction and get its index
            let undo_link = txn.append_undo_log(Arc::from(undo_log));

            // Create new meta with updated undo log index
            let mut new_meta = (*meta).clone();
            new_meta.set_undo_log_idx(txn.get_undo_log_num() - 1);
            let new_meta_arc = Arc::new(new_meta);

            // Update the version chain
            txn_manager.update_undo_link(rid, Some(undo_link), None);

            // Get write lock and perform update
            let page_guard = self.get_page(rid.get_page_id())?;
            let mut page = page_guard.write();
            return page
                .update_tuple(&new_meta_arc, tuple, rid)
                .map(|_| rid)
                .map_err(|e| format!("Failed to update tuple: {}", e));
        }

        // For non-transactional updates, get write lock and update
        let page_guard = self.get_page(rid.get_page_id())?;
        let mut page = page_guard.write();
        page.update_tuple(&meta, tuple, rid)
            .map(|_| rid)
            .map_err(|e| format!("Failed to update tuple: {}", e))
    }

    /// Updates the meta of a tuple.
    ///
    /// # Parameters
    ///
    /// - `meta`: New tuple meta.
    /// - `rid`: The RID of the inserted tuple.
    pub fn update_tuple_meta(&self, meta: Arc<TupleMeta>, rid: RID) -> Result<(), String> {
        let _write_guard = self.latch.write();

        // Get the page with write access
        let page_guard = self.get_page(rid.get_page_id())?;
        let mut page = page_guard.write();

        // First, get the current tuple meta to check visibility
        let (current_meta, _) = page.get_tuple(&rid, false)?;

        // Check visibility rules:
        // 1. If the current transaction created the tuple, allow update
        // 2. If the tuple is committed, allow update
        // 3. If the tuple is uncommitted and created by another transaction, deny update
        if current_meta.get_creator_txn_id() != meta.get_creator_txn_id()
            && !current_meta.is_committed()
        {
            return Err(
                "Cannot update meta of uncommitted tuple from another transaction".to_string(),
            );
        }

        // Now perform the update
        page.update_tuple_meta(&meta, &rid)
            .map_err(|e| format!("Failed to update tuple meta: {}", e))
    }

    pub fn get_bpm(&self) -> Arc<BufferPoolManager> {
        self.bpm.clone()
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
    pub fn get_tuple(&self, rid: RID) -> Result<(Arc<TupleMeta>, Arc<Tuple>), String> {
        let page_guard = self.get_page(rid.get_page_id())?;
        let page = page_guard.read();

        let (meta, tuple) = page.get_tuple(&rid, false)?;

        // Check if the tuple is deleted
        if meta.is_deleted() {
            return Err("Tuple is deleted".to_string());
        }

        // Allow access to both committed tuples and uncommitted tuples
        // The transaction visibility should be handled at a higher level
        Ok((Arc::new(meta), Arc::new(tuple)))
    }

    /// Gets a tuple with transaction visibility checks
    pub fn get_tuple_with_txn(
        &self,
        rid: RID,
        txn_ctx: Arc<TransactionContext>,
    ) -> Result<(Arc<TupleMeta>, Arc<Tuple>), String> {
        let txn = txn_ctx.get_transaction();

        // First get the latest version
        let page_guard = self.get_page(rid.get_page_id())?;
        let page = page_guard.read();
        let (meta, tuple) = page.get_tuple(&rid, false)?;

        // Check if the tuple is deleted
        if meta.is_deleted() {
            return Err("Tuple is deleted".to_string());
        }

        // Always allow transaction to see its own changes
        if meta.get_creator_txn_id() == txn.get_transaction_id() {
            return Ok((Arc::new(meta), Arc::new(tuple)));
        }

        match txn.get_isolation_level() {
            IsolationLevel::ReadUncommitted => {
                // For READ_UNCOMMITTED, return the latest version
                Ok((Arc::new(meta), Arc::new(tuple)))
            }

            IsolationLevel::ReadCommitted => {
                // For READ_COMMITTED, only return committed versions
                if meta.is_committed() {
                    Ok((Arc::new(meta), Arc::new(tuple)))
                } else {
                    Err("Tuple not visible".to_string())
                }
            }

            IsolationLevel::RepeatableRead | IsolationLevel::Serializable => {
                // For REPEATABLE_READ and SERIALIZABLE, check timestamp visibility
                if txn.is_tuple_visible(&meta) {
                    return Ok((Arc::new(meta), Arc::new(tuple)));
                }

                // If the latest version isn't visible, check the version chain
                let txn_manager = txn_ctx.get_transaction_manager();
                let mut current_link = txn_manager.get_undo_link(rid);

                while let Some(ref undo_link) = current_link {
                    let undo_log = txn_manager.get_undo_log(undo_link.clone());

                    let mut prev_meta = TupleMeta::new(undo_link.prev_txn);
                    prev_meta.set_commit_timestamp(undo_log.ts);
                    prev_meta.set_deleted(undo_log.is_deleted);

                    if txn.is_tuple_visible(&prev_meta) {
                        return Ok((Arc::new(prev_meta), undo_log.tuple.clone()));
                    }

                    // Get the next link from the undo log's prev_version
                    let prev_version = undo_log.prev_version.clone();
                    if !prev_version.is_valid() {
                        break;
                    }
                    current_link = Some(prev_version);
                }

                Err("No visible version found".to_string())
            }

            IsolationLevel::Snapshot => {
                // For SNAPSHOT, assume similar behavior to SERIALIZABLE for now
                // This may involve checking for a consistent snapshot version
                Ok((Arc::new(meta), Arc::new(tuple)))
            }
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
    pub fn get_tuple_meta(&self, rid: RID) -> Result<Arc<TupleMeta>, String> {
        let page_guard = self.get_page(rid.get_page_id())?;
        let page = page_guard.read();

        // Get the tuple meta
        let (meta, _) = page.get_tuple(&rid, false)?;

        // Check if the tuple is deleted
        if meta.is_deleted() {
            return Err("Tuple is deleted".to_string());
        }

        // Allow access to both committed and uncommitted tuple metadata
        // Transaction visibility should be handled at a higher level
        Ok(Arc::new(meta))
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
        if let Ok(page_guard) = self.get_page(current_page_id) {
            let page = page_guard.read();
            page.get_next_page_id()
        } else {
            INVALID_PAGE_ID
        }
    }

    /// Gets the first page ID (raw version without checking for tuples)
    fn get_first_page_id_raw(&self) -> PageId {
        *self.first_page_id.read()
    }

    /// Gets the first page ID that contains tuples
    pub fn get_first_page_id(&self) -> PageId {
        // Get the first page that has tuples
        let mut current_page_id = self.get_first_page_id_raw();

        // If no pages exist yet, return INVALID_PAGE_ID
        if current_page_id == INVALID_PAGE_ID {
            return INVALID_PAGE_ID;
        }

        // Find the first page with tuples
        while current_page_id != INVALID_PAGE_ID {
            if let Ok(page_guard) = self.get_page(current_page_id) {
                let page = page_guard.read();
                if page.get_num_tuples() > 0 {
                    return current_page_id;
                }
            }
            // Move to the next page
            current_page_id = self.get_next_page_id(current_page_id);
        }

        // If no pages with tuples found, return INVALID_PAGE_ID
        INVALID_PAGE_ID
    }

    /// Gets the last page ID (raw version without checking for tuples)
    fn get_last_page_id_raw(&self) -> PageId {
        *self.last_page_id.read()
    }

    /// Gets the last page ID that contains tuples
    pub fn get_last_page_id(&self) -> PageId {
        // Get the first page that has tuples
        let mut current_page_id = self.get_last_page_id_raw();

        // If no pages exist yet, return INVALID_PAGE_ID
        if current_page_id == INVALID_PAGE_ID {
            return INVALID_PAGE_ID;
        }

        // Find the first page with tuples
        while current_page_id != INVALID_PAGE_ID {
            if let Ok(page_guard) = self.get_page(current_page_id) {
                let page = page_guard.read();
                if page.get_num_tuples() > 0 {
                    return current_page_id;
                }
            }
            // Move to the next page
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
    /// A `PageGuard<TablePage>`.
    pub fn acquire_table_page_read_lock(&self, rid: RID) -> PageGuard<TablePage> {
        self.get_page(rid.get_page_id()).unwrap()
    }

    /// Acquires a write lock on a table page.
    ///
    /// # Parameters
    ///
    /// - `rid`: RID of the page to lock.
    ///
    /// # Returns
    ///
    /// A `PageGuard<TablePage>`.
    pub fn acquire_table_page_write_lock(&self, rid: RID) -> PageGuard<TablePage> {
        self.get_page(rid.get_page_id()).unwrap()
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
        page.get_tuple(&rid, false).unwrap()
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
        page.get_tuple(&rid, false).unwrap().0
    }

    /// Helper method to get the number of pages
    pub fn get_num_pages(&self) -> usize {
        let mut count = 0;
        let mut current_page_id = *self.first_page_id.read();

        while current_page_id != INVALID_PAGE_ID {
            count += 1;
            if let Ok(page_guard) = self.get_page(current_page_id) {
                let page = page_guard.read();
                current_page_id = page.get_next_page_id();
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
            if let Ok(page_guard) = self.get_page(current_page_id) {
                let page = page_guard.read();
                count += page.get_num_tuples() as usize;
                current_page_id = page.get_next_page_id();
            } else {
                break;
            }
        }
        count
    }

    /// Gets the table OID.
    pub fn get_table_oid(&self) -> TableOidT {
        self.table_oid
    }

    fn create_new_page_and_insert(&self, meta: &TupleMeta, tuple: &Tuple) -> Result<RID, String> {
        // First check if tuple is too large for any page
        let temp_page = TablePage::new(0); // Temporary page just for size check
        if temp_page.is_tuple_too_large(tuple) {
            return Err("Tuple is too large to fit in a page".to_string());
        }

        // Get the current last page ID before creating a new page
        let last_page_id = *self.last_page_id.read();

        // Create a new page
        let new_page_guard = self
            .bpm
            .new_page::<TablePage>()
            .ok_or_else(|| "Failed to create new page".to_string())?;
        let new_page_id = new_page_guard.read().get_page_id();

        debug!("Creating new page {} for tuple insertion", new_page_id);

        // Initialize the new page first
        {
            let mut new_page = new_page_guard.write();
            new_page.init();
            new_page.set_prev_page_id(last_page_id);
            new_page.set_dirty(true);
        }

        // Now get and update the last page
        {
            let last_page_guard = self.get_page(last_page_id)?;
            debug!("Current last page is {}, updating links", last_page_id);

            let mut last_page = last_page_guard.write();
            last_page.set_next_page_id(new_page_id);
            last_page.set_dirty(true);
        }

        // Update table heap's last page pointer
        *self.last_page_id.write() = new_page_id;

        debug!(
            "Page links updated, inserting tuple into new page {}",
            new_page_id
        );

        // Now insert the tuple into the new page
        let mut new_page = new_page_guard.write();

        // First verify the page has space for the tuple
        if !new_page.has_space_for(tuple) {
            debug!(
                "New page {} unexpectedly has no space for tuple",
                new_page_id
            );
            return Err(format!("New page {} has no space for tuple", new_page_id));
        }

        // Create the RID the tuple should have
        let new_rid = RID::new(new_page_id, 0);
        debug!(
            "Inserting tuple with RID {:?} into new page {}",
            new_rid, new_page_id
        );

        // Use the new method to bypass RID check
        match new_page.insert_tuple_with_rid(meta, tuple, new_rid) {
            Some(rid) => {
                new_page.set_dirty(true);
                debug!(
                    "Successfully inserted tuple into new page {}, RID: {:?}",
                    new_page_id, rid
                );
                Ok(rid)
            }
            None => {
                debug!(
                    "Failed to insert tuple into new page {} despite having space",
                    new_page_id
                );
                Err(format!(
                    "Failed to insert tuple into new page {} despite space check passing",
                    new_page_id
                ))
            }
        }
    }

    /// Create the first page for an empty table and insert the tuple
    fn create_first_page_and_insert(&self, meta: &TupleMeta, tuple: &Tuple) -> Result<RID, String> {
        // First check if tuple is too large for any page
        let temp_page = TablePage::new(0);
        if temp_page.is_tuple_too_large(tuple) {
            return Err("Tuple is too large to fit in a page".to_string());
        }

        // Create the first page
        let first_page_guard = self
            .bpm
            .new_page::<TablePage>()
            .ok_or_else(|| "Failed to create first page".to_string())?;
        let first_page_id = first_page_guard.read().get_page_id();

        debug!("Creating first page {} for empty table", first_page_id);

        // Initialize the first page
        {
            let mut page = first_page_guard.write();
            page.init();
            page.set_dirty(true);
        }

        // Update table heap's page pointers
        *self.first_page_id.write() = first_page_id;
        *self.last_page_id.write() = first_page_id;

        debug!("First page {} initialized, inserting tuple", first_page_id);

        // Now insert the tuple into the first page
        let mut page = first_page_guard.write();

        // Verify the page has space for the tuple
        if !page.has_space_for(tuple) {
            return Err("First page has no space for tuple".to_string());
        }

        // Insert the tuple
        match page.insert_tuple(meta, tuple) {
            Some(rid) => {
                page.set_dirty(true);
                debug!(
                    "Successfully inserted tuple into first page {}, RID: {:?}",
                    first_page_id, rid
                );
                Ok(rid)
            }
            None => {
                debug!("Failed to insert tuple into first page despite having space");
                Err("Failed to insert tuple into first page".to_string())
            }
        }
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageGuard<TablePage>, String> {
        self.bpm
            .fetch_page::<TablePage>(page_id)
            .ok_or_else(|| format!("Failed to fetch page {}", page_id))
    }

    /// Acquires a read lock on the entire table
    pub fn acquire_table_read_lock(&self) -> parking_lot::RwLockReadGuard<()> {
        self.latch.read()
    }

    /// Acquires a write lock on the entire table
    pub fn acquire_table_write_lock(&self) -> parking_lot::RwLockWriteGuard<()> {
        self.latch.write()
    }

    /// Acquires a read lock on a specific page
    pub fn acquire_page_read_lock(&self, rid: RID) -> Result<PageGuard<TablePage>, String> {
        self.get_page(rid.get_page_id())
    }

    /// Acquires a write lock on a specific page
    pub fn acquire_page_write_lock(&self, rid: RID) -> Result<PageGuard<TablePage>, String> {
        self.get_page(rid.get_page_id())
    }

    // Basic storage operations without transaction logic
    pub fn insert_tuple_internal(
        &self,
        meta: Arc<TupleMeta>,
        tuple: &Tuple,
    ) -> Result<RID, String> {
        let _write_guard = self.latch.write();

        // Check if we need to create the first page
        let last_page_id = *self.last_page_id.read();
        if last_page_id == INVALID_PAGE_ID {
            // Table is empty, create the first page
            return self.create_first_page_and_insert(&meta, tuple);
        }

        // Get the last page
        let page_guard = self.get_page(last_page_id)?;

        // Try to insert into the current page
        {
            let mut page = page_guard.write();
            if page.has_space_for(tuple) {
                return if let Some(rid) = page.insert_tuple(&meta, tuple) {
                    page.set_dirty(true);
                    Ok(rid)
                } else {
                    Err("Failed to insert tuple into page".to_string())
                }
            }
        }

        // If we get here, we need a new page
        self.create_new_page_and_insert(&meta, tuple)
    }

    pub fn update_tuple_internal(
        &self,
        meta: Arc<TupleMeta>,
        tuple: &Tuple,
        rid: RID,
    ) -> Result<RID, String> {
        let _write_guard = self.latch.write();

        let page_guard = self.get_page(rid.get_page_id())?;
        let mut page = page_guard.write();

        page.update_tuple(&meta, tuple, rid)
            .map(|_| rid)
            .map_err(|e| format!("Failed to update tuple: {}", e))
    }

    pub fn get_tuple_internal(&self, rid: RID, allow_deleted: bool) -> Result<(Arc<TupleMeta>, Arc<Tuple>), String> {
        let page_guard = self.get_page(rid.get_page_id())?;
        let page = page_guard.read();
        let (meta, tuple) = page.get_tuple(&rid, allow_deleted)?;
        Ok((Arc::new(meta), Arc::new(tuple)))
    }

    /// Inserts a tuple from values and schema directly without requiring a pre-existing tuple.
    ///
    /// # Parameters
    ///
    /// - `values`: Values for the tuple.
    /// - `schema`: Schema for the tuple.
    /// - `meta`: Tuple metadata.
    ///
    /// # Returns
    ///
    /// An `Result` containing the RID of the inserted tuple, or an error message.
    pub fn insert_tuple_from_values(
        &self,
        values: Vec<Value>,
        schema: &Schema,
        meta: Arc<TupleMeta>,
    ) -> Result<RID, String> {
        // Expand values to match schema, handling AUTO_INCREMENT and DEFAULT values
        let expanded_values = self.expand_values_for_schema(values, schema)?;
        
        let _write_guard = self.latch.write();

        // Get the last page
        let last_page_id = *self.last_page_id.read();
        let page_guard = self.get_page(last_page_id)?;

        // Create a temporary tuple to check size (with dummy RID)
        let temp_tuple = Tuple::new(&expanded_values, &schema, RID::default());

        {
            let mut page = page_guard.write();

            // Check if page has space
            if page.has_space_for(&temp_tuple) {
                // Get the next RID
                let next_rid = page.get_next_rid();

                // Create the tuple with the correct RID
                let tuple = Tuple::new(&expanded_values, &schema, next_rid);

                // Insert the tuple
                match page.insert_tuple_with_rid(&meta, &tuple, next_rid) {
                    Some(rid) => {
                        page.set_dirty(true);
                        return Ok(rid);
                    }
                    None => {
                        // Insertion failed despite having space
                        debug!("Page reported having space but insert failed, trying new page");
                    }
                }
            }
        }

        // If we get here, we need a new page
        self.create_new_page_and_insert_from_values(expanded_values, schema, &meta)
    }

    /// Expands provided values to match the full schema by handling AUTO_INCREMENT and DEFAULT values
    pub fn expand_values_for_schema(
        &self,
        values: Vec<Value>,
        schema: &Schema,
    ) -> Result<Vec<Value>, String> {
        let schema_column_count = schema.get_column_count() as usize;
        
        // If we have more values than columns, that's an error
        if values.len() > schema_column_count {
            return Err(format!(
                "Too many values provided: {} values for {} columns",
                values.len(),
                schema_column_count
            ));
        }
        
        let mut expanded_values = Vec::with_capacity(schema_column_count);
        
        // Process each column in the schema
        for i in 0..schema_column_count {
            let column = schema.get_column(i).ok_or_else(|| {
                format!("Column index {} out of bounds in schema", i)
            })?;
            
            if i < values.len() {
                // We have a value for this position
                let value = &values[i];
                
                // Check if this is a NULL value for an AUTO_INCREMENT column
                if value.is_null() && column.is_primary_key() {
                    // Replace NULL with auto-generated value for AUTO_INCREMENT primary keys
                    let auto_increment_value = self.get_next_auto_increment_value()?;
                    expanded_values.push(Value::new(auto_increment_value));
                } else {
                    // Use the provided value as-is
                    expanded_values.push(value.clone());
                }
            } else {
                // We need to generate a value for this column (missing value)
                if column.is_primary_key() {
                    // For AUTO_INCREMENT primary keys, generate the next value
                    let auto_increment_value = self.get_next_auto_increment_value()?;
                    expanded_values.push(Value::new(auto_increment_value));
                } else if let Some(default_value) = column.get_default_value() {
                    // Use the column's default value
                    if default_value.is_current_timestamp() {
                        // Handle CURRENT_TIMESTAMP default
                        let current_time = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map_err(|e| format!("Failed to get current timestamp: {}", e))?
                            .as_secs();
                        expanded_values.push(Value::new(current_time));
                    } else {
                        expanded_values.push(default_value.clone());
                    }
                } else {
                    // No default value, use NULL (or error if NOT NULL)
                    if column.is_not_null() {
                        return Err(format!(
                            "Column '{}' cannot be NULL and has no default value",
                            column.get_name()
                        ));
                    }
                    expanded_values.push(Value::new_with_type(
                        crate::types_db::value::Val::Null,
                        column.get_type(),
                    ));
                }
            }
        }
        
        Ok(expanded_values)
    }
    
    /// Gets the next auto-increment value for this table
    /// For now, this is a simple implementation - in a real system this would
    /// be tracked per table and persisted
    fn get_next_auto_increment_value(&self) -> Result<i32, String> {
        // Simple implementation: start from 1 and increment
        // In a real implementation, this should be tracked per table
        static AUTO_INCREMENT_COUNTER: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(1);
        Ok(AUTO_INCREMENT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst))
    }

    // Helper method to create a new page and insert values
    fn create_new_page_and_insert_from_values(
        &self,
        values: Vec<Value>,
        schema: &Schema,
        meta: &TupleMeta,
    ) -> Result<RID, String> {
        // First check if tuple is too large for any page
        let temp_tuple = Tuple::new(&values, &schema, RID::default());
        let temp_page = TablePage::new(0);
        if temp_page.is_tuple_too_large(&temp_tuple) {
            return Err("Tuple is too large to fit in a page".to_string());
        }

        // Get the current last page ID before creating a new page
        let last_page_id = *self.last_page_id.read();

        // Create a new page
        let new_page_guard = self
            .bpm
            .new_page::<TablePage>()
            .ok_or_else(|| "Failed to create new page".to_string())?;
        let new_page_id = new_page_guard.read().get_page_id();

        debug!("Creating new page {} for tuple insertion", new_page_id);

        // Initialize the new page first
        {
            let mut new_page = new_page_guard.write();
            new_page.init();
            new_page.set_prev_page_id(last_page_id);
            new_page.set_dirty(true);
        }

        // Now get and update the last page
        {
            let last_page_guard = self.get_page(last_page_id)?;
            debug!("Current last page is {}, updating links", last_page_id);

            let mut last_page = last_page_guard.write();
            last_page.set_next_page_id(new_page_id);
            last_page.set_dirty(true);
        }

        // Update table heap's last page pointer
        *self.last_page_id.write() = new_page_id;

        debug!(
            "Page links updated, inserting tuple into new page {}",
            new_page_id
        );

        // Create the tuple with the correct RID for the new page
        let new_rid = RID::new(new_page_id, 0);
        let tuple = Tuple::new(&values, &schema, new_rid);

        // Now insert the tuple into the new page
        let mut new_page = new_page_guard.write();

        // First verify the page has space for the tuple
        if !new_page.has_space_for(&tuple) {
            debug!(
                "New page {} unexpectedly has no space for tuple",
                new_page_id
            );
            return Err(format!("New page {} has no space for tuple", new_page_id));
        }

        debug!(
            "Inserting tuple with RID {:?} into new page {}",
            new_rid, new_page_id
        );

        // Use the insert_tuple_with_rid method to bypass RID check
        match new_page.insert_tuple_with_rid(meta, &tuple, new_rid) {
            Some(rid) => {
                new_page.set_dirty(true);
                debug!(
                    "Successfully inserted tuple into new page {}, RID: {:?}",
                    new_page_id, rid
                );
                Ok(rid)
            }
            None => {
                debug!(
                    "Failed to insert tuple into new page {} despite having space",
                    new_page_id
                );
                Err(format!(
                    "Failed to insert tuple into new page {} despite space check passing",
                    new_page_id
                ))
            }
        }
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
    pub fn new(schema: Schema, name: String, table: Arc<TableHeap>, oid: TableOidT) -> Self {
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

    pub fn insert_tuple(&self, meta: &TupleMeta, tuple: &mut Tuple) -> Result<RID, String> {
        // Get the last page or create a new one if none exists
        let page_guard = if *self.last_page_id.read() == INVALID_PAGE_ID {
            // Create the first page
            let new_page = self
                .bpm
                .new_page::<TablePage>()
                .ok_or("Failed to create new table page")?;

            // Initialize the page
            {
                let mut page = new_page.write();
                page.init();
                page.set_dirty(true);
            }

            // Update page IDs
            *self.first_page_id.write() = new_page.get_page_id();
            *self.last_page_id.write() = new_page.get_page_id();

            new_page
        } else {
            // Get existing last page
            self.bpm
                .fetch_page::<TablePage>(*self.last_page_id.read())
                .ok_or("Failed to fetch last page")?
        };

        let mut page = page_guard.write();

        // Try to insert into the current page
        if page.has_space_for(&mut *tuple) {
            let rid = page
                .insert_tuple(meta, tuple)
                .ok_or("Failed to insert tuple into page")?;
            page.set_dirty(true);
            Ok(rid)
        } else {
            // The current page is full, create a new page
            drop(page); // Release write lock on the current page

            let new_page_guard = self
                .bpm
                .new_page::<TablePage>()
                .ok_or("Failed to create new table page")?;

            let new_page_id = new_page_guard.get_page_id();

            // Initialize the new page
            {
                let mut new_page = new_page_guard.write();
                new_page.init();

                // Set up page links
                new_page.set_prev_page_id(*self.last_page_id.read());
                new_page.set_dirty(true);

                // Try to insert the tuple
                let rid = new_page
                    .insert_tuple(meta, tuple)
                    .ok_or("Failed to insert tuple into new page")?;

                // Update the previous page's next pointer
                if let Some(prev_guard) =
                    self.bpm.fetch_page::<TablePage>(*self.last_page_id.read())
                {
                    let mut prev_page = prev_guard.write();
                    prev_page.set_next_page_id(new_page_id);
                    prev_page.set_dirty(true);
                }

                // Update last page pointer
                *self.last_page_id.write() = new_page_id;

                Ok(rid)
            }
        }
    }

    pub fn update_tuple(
        &self,
        meta: &TupleMeta,
        tuple: &mut Tuple,
        rid: RID,
    ) -> Result<(), PageError> {
        let page_guard = self
            .bpm
            .fetch_page::<TablePage>(rid.get_page_id())
            .ok_or(PageError::NoPageReference)?;

        let mut page = page_guard.write();
        page.update_tuple(meta, tuple, rid)
    }

    pub fn get_page(&self, page_id: PageId) -> Result<PageGuard<TablePage>, String> {
        self.bpm
            .fetch_page::<TablePage>(page_id)
            .ok_or_else(|| format!("Failed to fetch page {}", page_id))
    }
}

impl Debug for TableHeap {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TableHeap")
            .field("first_page_id", &self.first_page_id.read())
            .field("last_page_id", &self.last_page_id.read())
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
            first_page_id: RwLock::new(*self.first_page_id.read()),
            last_page_id: RwLock::new(*self.last_page_id.read()),
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
            bpm: self.bpm.clone(),
            first_page_id: RwLock::new(*self.first_page_id.read()),
            last_page_id: RwLock::new(*self.last_page_id.read()),
            table_oid: self.table_oid,
            latch: RwLock::new(()),
        }
    }
}
