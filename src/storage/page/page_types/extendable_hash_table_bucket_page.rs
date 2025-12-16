use crate::common::config::{storage_bincode_config, DB_PAGE_SIZE, PageId};
use crate::common::exception::PageError;
use crate::common::rid::RID;
use crate::storage::page::{PAGE_TYPE_OFFSET, Page, PageTrait, PageType, PageTypeId};
use crate::types_db::value::Value;
use std::any::Any;
use std::fmt::Debug;
use std::mem::size_of;

pub const HTABLE_BUCKET_PAGE_METADATA_SIZE: usize = size_of::<u32>() * 2;
pub const BUCKET_HEADER_SIZE: usize = size_of::<PageId>() + size_of::<u16>() * 2;

#[derive(Debug)]
pub struct ExtendableHTableBucketPage {
    data: Box<[u8; DB_PAGE_SIZE as usize]>,
    page_id: PageId,
    pin_count: i32,
    is_dirty: bool,
    local_depth: u8,
    size: u16,
    max_size: u16,
    entries: Vec<(Value, RID)>,
}

impl ExtendableHTableBucketPage {
    pub fn init(&mut self, max_size: u16) {
        self.max_size = max_size;
        self.entries.reserve(max_size as usize);
    }

    pub fn insert(&mut self, key: Value, value: RID) -> bool {
        if self.is_full() {
            return false;
        }

        self.entries.push((key, value));
        self.size += 1;
        self.is_dirty = true;
        true
    }

    pub fn remove(&mut self, key: &Value) -> bool {
        if let Some(pos) = self.entries.iter().position(|(k, _)| k == key) {
            self.entries.remove(pos);
            self.size -= 1;
            self.is_dirty = true;
            true
        } else {
            false
        }
    }

    pub fn lookup(&self, key: &Value) -> Option<RID> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, rid)| *rid)
    }

    pub fn is_full(&self) -> bool {
        self.size >= self.max_size
    }

    pub fn is_empty(&self) -> bool {
        self.size == 0
    }

    pub fn get_size(&self) -> u16 {
        self.size
    }

    pub fn get_local_depth(&self) -> u8 {
        self.local_depth
    }

    pub fn set_local_depth(&mut self, depth: u8) {
        self.local_depth = depth;
        self.is_dirty = true;
    }

    pub fn get_all_entries(&self) -> Vec<(Value, RID)> {
        self.entries.clone()
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.size = 0;
        self.is_dirty = true;
    }

    /// Serializes the bucket page data
    #[allow(dead_code)]
    fn serialize_entries(&mut self) -> Result<(), PageError> {
        // Start after the page header
        let mut offset = BUCKET_HEADER_SIZE;

        // Write local depth
        self.data[offset] = self.local_depth;
        offset += 1;

        // Write size and max_size
        self.data[offset..offset + 2].copy_from_slice(&self.size.to_le_bytes());
        offset += 2;
        self.data[offset..offset + 2].copy_from_slice(&self.max_size.to_le_bytes());
        offset += 2;

        // Write entries
        for (value, rid) in &self.entries {
            // Serialize value
            let value_bytes = bincode::encode_to_vec(value, storage_bincode_config())
                .map_err(|_| PageError::SerializationError)?;
            let value_size = value_bytes.len();

            // Write value size and data
            self.data[offset..offset + 4].copy_from_slice(&(value_size as u32).to_le_bytes());
            offset += 4;
            self.data[offset..offset + value_size].copy_from_slice(&value_bytes);
            offset += value_size;

            // Write RID
            self.data[offset..offset + 8].copy_from_slice(&rid.get_page_id().to_le_bytes());
            offset += 8;
            self.data[offset..offset + 4].copy_from_slice(&rid.get_slot_num().to_le_bytes());
            offset += 4;
        }

        Ok(())
    }

    /// Deserializes the bucket page data
    fn deserialize_entries(&mut self) -> Result<(), PageError> {
        let mut offset = BUCKET_HEADER_SIZE;

        // Read local depth
        self.local_depth = self.data[offset];
        offset += 1;

        // Read size and max_size
        self.size = u16::from_le_bytes(self.data[offset..offset + 2].try_into().unwrap());
        offset += 2;
        self.max_size = u16::from_le_bytes(self.data[offset..offset + 2].try_into().unwrap());
        offset += 2;

        // Clear and read entries
        self.entries.clear();
        for _ in 0..self.size {
            // Read value size
            let value_size = u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap());
            offset += 4;

            // Deserialize value
            let (value, _): (Value, _) = bincode::decode_from_slice(
                &self.data[offset..offset + value_size as usize],
                storage_bincode_config(),
            )
            .map_err(|_| PageError::DeserializationError)?;
            offset += value_size as usize;

            // Read RID
            let page_id = PageId::from_le_bytes(self.data[offset..offset + 8].try_into().unwrap());
            offset += 8;
            let slot_num = u32::from_le_bytes(self.data[offset..offset + 4].try_into().unwrap());
            offset += 4;

            let rid = RID::new(page_id, slot_num);
            self.entries.push((value, rid));
        }

        Ok(())
    }
}

impl PageTypeId for ExtendableHTableBucketPage {
    const TYPE_ID: PageType = PageType::HashTableBucket;
}

impl Page for ExtendableHTableBucketPage {
    fn new(page_id: PageId) -> Self {
        let mut page = Self {
            data: Box::new([0; DB_PAGE_SIZE as usize]),
            page_id,
            pin_count: 1,
            is_dirty: false,
            local_depth: 0,
            size: 0,
            max_size: 0,
            entries: Vec::new(),
        };
        page.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
        page
    }
}

impl PageTrait for ExtendableHTableBucketPage {
    fn get_page_id(&self) -> PageId {
        self.page_id
    }

    fn get_page_type(&self) -> PageType {
        PageType::from_u8(self.data[PAGE_TYPE_OFFSET]).unwrap_or(PageType::Invalid)
    }

    fn is_dirty(&self) -> bool {
        self.is_dirty
    }

    fn set_dirty(&mut self, is_dirty: bool) {
        self.is_dirty = is_dirty;
    }

    fn get_pin_count(&self) -> i32 {
        self.pin_count
    }

    fn increment_pin_count(&mut self) {
        self.pin_count += 1;
    }

    fn decrement_pin_count(&mut self) {
        if self.pin_count > 0 {
            self.pin_count -= 1;
        }
    }

    fn get_data(&self) -> &[u8; DB_PAGE_SIZE as usize] {
        &self.data
    }

    fn get_data_mut(&mut self) -> &mut [u8; DB_PAGE_SIZE as usize] {
        &mut self.data
    }

    fn set_data(&mut self, offset: usize, new_data: &[u8]) -> Result<(), PageError> {
        if offset + new_data.len() > self.data.len() {
            return Err(PageError::InvalidOffset {
                offset,
                page_size: self.data.len(),
            });
        }
        self.data[offset..offset + new_data.len()].copy_from_slice(new_data);
        self.is_dirty = true;

        // If we're setting the whole page data, try to deserialize entries
        if offset == 0 && new_data.len() == self.data.len() {
            self.deserialize_entries()?;
        }
        Ok(())
    }

    fn set_pin_count(&mut self, pin_count: i32) {
        self.pin_count = pin_count;
    }

    fn reset_memory(&mut self) {
        self.data.fill(0);
        self.entries.clear();
        self.size = 0;
        self.local_depth = 0;
        self.is_dirty = false;
        self.data[PAGE_TYPE_OFFSET] = Self::TYPE_ID.to_u8();
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }
}

#[cfg(test)]
mod basic_behavior {
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
    use crate::storage::page::page_types::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
    use log::info;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use std::thread;
    use tempfile::TempDir;

    pub struct TestContext {
        bpm: Arc<BufferPoolManager>,
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
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
            let disk_manager =
                AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(
                BufferPoolManager::new(
                    BUFFER_POOL_SIZE,
                    Arc::from(disk_manager.unwrap()),
                    replacer.clone(),
                )
                .unwrap(),
            );

            Self { bpm }
        }
    }

    #[tokio::test]
    async fn bucket_page_integrity() {
        let ctx = TestContext::new("bucket_page_integrity").await;
        let bpm = &ctx.bpm;

        // Create a new bucket page using type-safe API
        let bucket_page = bpm
            .new_page::<ExtendableHTableBucketPage>()
            .expect("Failed to create new bucket page");

        {
            let mut page = bucket_page.write();
            let bucket_page_size = page.get_size();
            info!("bucket page size: {}", bucket_page_size);

            page.init(10);
            info!("Initialized bucket page with max size: {}", page.max_size);
            assert_eq!(page.max_size, 10, "Max size should be 10");
            assert_eq!(page.get_size(), 0, "Initial size should be 0");
        }
    }

    #[tokio::test]
    async fn end_to_end() {
        let ctx = TestContext::new("test_basic_behaviour").await;
        let bpm = &ctx.bpm;

        info!("Starting basic behaviour tests");

        let mut bucket_pages = Vec::new();

        // Create bucket pages
        for _ in 0..4 {
            let bucket_page = bpm
                .new_page::<ExtendableHTableBucketPage>()
                .expect("Failed to create new bucket page");

            {
                let mut page = bucket_page.write();
                page.init(10);
                info!("Initialized bucket page with max size: {}", page.get_size());
            }

            bucket_pages.push(bucket_page);
        }

        // Create directory page
        let directory_page = bpm
            .new_page::<ExtendableHTableDirectoryPage>()
            .expect("Failed to create directory page");

        {
            let mut dir = directory_page.write();
            dir.init(3);
            info!("Initialized directory page with max depth: 3");

            // Get page IDs from bucket pages
            let bucket_page_ids: Vec<PageId> = bucket_pages
                .iter()
                .map(|p| p.read().get_page_id())
                .collect();

            dir.set_bucket_page_id(0, bucket_page_ids[0]);

            /*
            ======== DIRECTORY (global_depth_: 0) ========
            | bucket_idx | page_id | local_depth |
            |    0       |    0    |    0        |
            ================ END DIRECTORY ================
            */
            dir.print_directory();
            dir.verify_integrity();
            assert_eq!(dir.get_size(), 1);
            assert_eq!(dir.get_bucket_page_id(0), Some(bucket_page_ids[0]));

            // Continue with the rest of the test...
            // Update the remaining directory operations using the type-safe approach
        }
    }

    #[tokio::test]
    async fn concurrent_access() {
        let ctx = Arc::new(TestContext::new("test_concurrent_access").await);
        let bpm = &ctx.bpm;

        info!("Starting concurrent access tests");

        let num_threads = 4;
        let operations_per_thread = 2; // Reduced to fit within buffer pool limits

        let mut handles = vec![];

        for i in 0..num_threads {
            let bpm_clone = Arc::clone(bpm);
            let handle = thread::spawn(move || {
                for j in 0..operations_per_thread {
                    let bucket_page = bpm_clone
                        .new_page::<ExtendableHTableBucketPage>()
                        .expect("Failed to create new bucket page");

                    let bucket_page_id = {
                        let page = bucket_page.read();
                        page.get_page_id()
                    };

                    info!(
                        "Thread {} created bucket page with ID: {} (operation {})",
                        i, bucket_page_id, j
                    );
                }
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().expect("Failed to join thread");
        }

        info!("Concurrent access tests completed successfully");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_page_type() {
        let page = ExtendableHTableBucketPage::new(1);
        assert_eq!(page.get_page_type(), PageType::HashTableBucket);

        // Test after reset
        let mut page = ExtendableHTableBucketPage::new(1);
        page.reset_memory();
        assert_eq!(page.get_page_type(), PageType::HashTableBucket);
    }

    #[test]
    fn test_serialization() {
        let mut page = ExtendableHTableBucketPage::new(1);
        page.init(10);

        // Add some test entries
        let value = Value::new(42);
        let rid = RID::new(1, 1);
        page.insert(value.clone(), rid);

        // Serialize
        page.serialize_entries().unwrap();

        // Create new page and copy data
        let mut new_page = ExtendableHTableBucketPage::new(2);
        new_page.set_data(0, page.get_data()).unwrap();

        // Verify entries were deserialized correctly
        assert_eq!(new_page.get_size(), 1);
        assert_eq!(new_page.lookup(&value), Some(rid));
    }

    #[test]
    fn test_data_operations() {
        let mut page = ExtendableHTableBucketPage::new(1);

        // Test data setting
        let test_data = vec![1, 2, 3, 4];
        page.set_data(10, &test_data).unwrap();

        // Verify data was set
        assert_eq!(&page.get_data()[10..14], &test_data);
        assert!(page.is_dirty());
    }
}
