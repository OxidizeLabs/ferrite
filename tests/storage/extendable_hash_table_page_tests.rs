use std::sync::Arc;
use tkdb::common::config::{INVALID_PAGE_ID, PageId};
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::common::rid::RID;
use tkdb::storage::disk::disk_manager::DiskManager;
use tkdb::storage::index::generic_key::GenericKey;
use tkdb::storage::page::extendable_hash_table_bucket_page::ExtendableHTableBucketPage;
use tkdb::storage::page::extendable_hash_table_directory_page::ExtendableHTableDirectoryPage;
use tkdb::storage::page::extendable_hash_table_header_page::ExtendableHTableHeaderPage;

#[cfg(test)]
mod tests {
    use std::fs::remove_file;
    use super::*;
    use std::mem::size_of;
    use spin::Mutex;
    use tkdb::buffer::lru_k_replacer::LRUKReplacer;
    use tkdb::catalogue::column::Column;
    use tkdb::catalogue::schema::Schema;
    use tkdb::storage::disk::disk_scheduler::DiskScheduler;
    use tkdb::storage::index::generic_key::GenericComparator;
    use tkdb::types_db::type_id::TypeId;

    const PAGE_ID_SIZE: usize = size_of::<PageId>();

    struct TestContext {
        disk_manager: Arc<DiskManager>,
        disk_scheduler: Arc<DiskScheduler>,
        replacer: Arc<Mutex<LRUKReplacer>>,
        db_file: String,
        db_log: String,
    }

    impl TestContext {
        fn new() -> Self {
            let db_file = "test_bpm.db";
            let log_file = "test_bpm.log";
            let disk_manager = Arc::new(DiskManager::new(db_file, log_file));
            let disk_scheduler = Arc::new(DiskScheduler::new(Arc::clone(&disk_manager)));
            let replacer = Arc::new(Mutex::new(LRUKReplacer::new(7, 2)));

            Self {
                disk_manager,
                disk_scheduler,
                replacer,
                db_file: db_file.to_string(),
                db_log: log_file.to_string(),
            }
        }

        fn cleanup(&self) {
            let _ = remove_file(&self.db_file);
            let _ = remove_file(&self.db_log);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup();
        }
    }

    #[test]
    fn test_page_id_size() {
        assert_eq!(PAGE_ID_SIZE, 8, "PageId size is not 4 bytes");
    }

    #[tokio::test]
    async fn bucket_page_sample_test() {
        let ctx = TestContext::new();
        let buffer_pool_size = 10;
        let bpm = Arc::new(BufferPoolManager::new(
            buffer_pool_size,
            ctx.disk_scheduler.clone(),
            ctx.disk_manager.clone(),
            ctx.replacer.clone(),
        ));


        let guard = bpm.new_page_guarded().unwrap();
        let mut guard = guard;
        let bucket_page: &mut ExtendableHTableBucketPage<GenericKey<8>, RID, GenericComparator> =
            guard.as_type_mut::<ExtendableHTableBucketPage<GenericKey<8>, RID, GenericComparator>>();
        bucket_page.init(10);
        let key_schema = Arc::new(Schema::new(vec![Column::new("a".to_string(), TypeId::BigInt)]));
        let comparator = GenericComparator::new(key_schema.clone());
        let mut index_key = GenericKey::<8>::new();
        let mut rid = RID::default();

        // insert a few (key, value) pairs
        for i in 0..10 {
            index_key.set_from_integer(i);
            rid.set(i as PageId, i.try_into().unwrap());
            assert!(bucket_page.insert(index_key.clone(), rid, &comparator));
        }

        index_key.set_from_integer(11);
        rid.set(11, 11);
        assert!(bucket_page.is_full());
        assert!(!bucket_page.insert(index_key.clone(), rid, &comparator));

        // check for the inserted pairs
        for i in 0..10 {
            index_key.set_from_integer(i);
            let found_rid = bucket_page.lookup(&index_key, &comparator);
            assert_eq!(found_rid, Some(RID::with_page_and_slot(i, i as u32)));
        }

        // remove a few pairs
        for i in 0..10 {
            if i % 2 == 1 {
                index_key.set_from_integer(i);
                assert!(bucket_page.remove(&index_key, &comparator));
            }
        }

        for i in 0..10 {
            index_key.set_from_integer(i);
            if i % 2 == 1 {
                // remove the same pairs again
                assert!(!bucket_page.remove(&index_key, &comparator));
            } else {
                assert!(bucket_page.remove(&index_key, &comparator));
            }
        }

        assert!(bucket_page.is_empty());
    }


    #[tokio::test]
    async fn header_directory_page_sample_test() {
        let ctx = TestContext::new();
        let buffer_pool_size = 10;
        let bpm = Arc::new(BufferPoolManager::new(
            buffer_pool_size,
            ctx.disk_scheduler.clone(),
            ctx.disk_manager.clone(),
            ctx.replacer.clone(),
        ));

        let header_page_id = INVALID_PAGE_ID;
        let directory_page_id = INVALID_PAGE_ID;
        let bucket_page_id_1 = INVALID_PAGE_ID;
        let bucket_page_id_2 = INVALID_PAGE_ID;
        let bucket_page_id_3 = INVALID_PAGE_ID;
        let bucket_page_id_4 = INVALID_PAGE_ID;
        {
            /************************ HEADER PAGE TEST ************************/
            let mut header_guard = bpm.new_page_guarded().unwrap();
            let header_page = header_guard.as_type_mut::<ExtendableHTableHeaderPage>();
            header_page.init(2);

            /* Test hashes for header page
            00000000000000001000000000000000 - 32768
            01000000000000001000000000000000 - 1073774592
            10000000000000001000000000000000 - 2147516416
            11000000000000001000000000000000 - 3221258240
            */

            // ensure we are hashing into proper bucket based on upper 2 bits
            let hashes = [32768, 1073774592, 2147516416, 3221258240];
            for i in 0..4 {
                assert_eq!(header_page.hash_to_directory_index(hashes[i]), i as u32);
            }

            header_guard.drop();

            /************************ DIRECTORY PAGE TEST ************************/
            let mut directory_guard = bpm.new_page_guarded().unwrap();
            let directory_page = directory_guard.as_type_mut::<ExtendableHTableDirectoryPage>();
            directory_page.init(3);

            let mut bucket_guard_1 = bpm.new_page_guarded().unwrap();
            let bucket_page_1 = bucket_guard_1.as_type_mut::<ExtendableHTableBucketPage<GenericKey<8>, RID, GenericComparator>>();
            bucket_page_1.init(10);

            let mut bucket_guard_2 = bpm.new_page_guarded().unwrap();
            let bucket_page_2 = bucket_guard_2.as_type_mut::<ExtendableHTableBucketPage<GenericKey<8>, RID, GenericComparator>>();
            bucket_page_2.init(10);

            let mut bucket_guard_3 = bpm.new_page_guarded().unwrap();
            let bucket_page_3 = bucket_guard_3.as_type_mut::<ExtendableHTableBucketPage<GenericKey<8>, RID, GenericComparator>>();
            bucket_page_3.init(10);

            let mut bucket_guard_4 = bpm.new_page_guarded().unwrap();
            let bucket_page_4 = bucket_guard_4.as_type_mut::<ExtendableHTableBucketPage<GenericKey<8>, RID, GenericComparator>>();
            bucket_page_4.init(10);

            directory_page.set_bucket_page_id(0, bucket_page_id_1);

            /*
            ======== DIRECTORY (global_depth_: 0) ========
            | bucket_idx | page_id | local_depth |
            |    0       |    2    |    0        |
            ================ END DIRECTORY ================
            */

            directory_page.verify_integrity();
            assert_eq!(directory_page.size(), 1);
            assert_eq!(directory_page.get_bucket_page_id(0), bucket_page_id_1);

            // grow the directory, local depths should change!
            directory_page.set_local_depth(0, 1);
            directory_page.incr_global_depth();
            directory_page.set_bucket_page_id(1, bucket_page_id_2);
            directory_page.set_local_depth(1, 1);

            /*
            ======== DIRECTORY (global_depth_: 1) ========
            | bucket_idx | page_id | local_depth |
            |    0       |    2    |    1        |
            |    1       |    3    |    1        |
            ================ END DIRECTORY ================
            */

            directory_page.verify_integrity();
            assert_eq!(directory_page.size(), 2);
            assert_eq!(directory_page.get_bucket_page_id(0), bucket_page_id_1);
            assert_eq!(directory_page.get_bucket_page_id(1), bucket_page_id_2);

            for i in 0..100 {
                assert_eq!(directory_page.hash_to_bucket_index(i), i % 2);
            }

            directory_page.set_local_depth(0, 2);
            directory_page.incr_global_depth();
            directory_page.set_bucket_page_id(2, bucket_page_id_3);

            /*
            ======== DIRECTORY (global_depth_: 2) ========
            | bucket_idx | page_id | local_depth |
            |    0       |    2    |    2        |
            |    1       |    3    |    1        |
            |    2       |    4    |    2        |
            |    3       |    3    |    1        |
            ================ END DIRECTORY ================
            */

            directory_page.verify_integrity();
            assert_eq!(directory_page.size(), 4);
            assert_eq!(directory_page.get_bucket_page_id(0), bucket_page_id_1);
            assert_eq!(directory_page.get_bucket_page_id(1), bucket_page_id_2);
            assert_eq!(directory_page.get_bucket_page_id(2), bucket_page_id_3);
            assert_eq!(directory_page.get_bucket_page_id(3), bucket_page_id_2);

            for i in 0..100 {
                assert_eq!(directory_page.hash_to_bucket_index(i), i % 4);
            }

            directory_page.set_local_depth(0, 3);
            directory_page.incr_global_depth();
            directory_page.set_bucket_page_id(4, bucket_page_id_4);

            /*
            ======== DIRECTORY (global_depth_: 3) ========
            | bucket_idx | page_id | local_depth |
            |    0       |    2    |    3        |
            |    1       |    3    |    1        |
            |    2       |    4    |    2        |
            |    3       |    3    |    1        |
            |    4       |    5    |    3        |
            |    5       |    3    |    1        |
            |    6       |    4    |    2        |
            |    7       |    3    |    1        |
            ================ END DIRECTORY ================
            */
            directory_page.verify_integrity();
            assert_eq!(directory_page.size(), 8);
            assert_eq!(directory_page.get_bucket_page_id(0), bucket_page_id_1);
            assert_eq!(directory_page.get_bucket_page_id(1), bucket_page_id_2);
            assert_eq!(directory_page.get_bucket_page_id(2), bucket_page_id_3);
            assert_eq!(directory_page.get_bucket_page_id(3), bucket_page_id_2);
            assert_eq!(directory_page.get_bucket_page_id(4), bucket_page_id_4);
            assert_eq!(directory_page.get_bucket_page_id(5), bucket_page_id_2);
            assert_eq!(directory_page.get_bucket_page_id(6), bucket_page_id_3);
            assert_eq!(directory_page.get_bucket_page_id(7), bucket_page_id_2);

            for i in 0..100 {
                assert_eq!(directory_page.hash_to_bucket_index(i), i % 8);
            }

            // uncommenting this code line below should cause an "Assertion failed"
            // since this would be exceeding the max depth we initialized
            // directory_page.incr_global_depth();

            // at this time, we cannot shrink the directory since we have ld = gd = 3
            assert_eq!(directory_page.can_shrink(), false);

            directory_page.set_local_depth(0, 2);
            directory_page.set_local_depth(4, 2);
            directory_page.set_bucket_page_id(0, bucket_page_id_4);

            /*
            ======== DIRECTORY (global_depth_: 3) ========
            | bucket_idx | page_id | local_depth |
            |    0       |    5    |    2        |
            |    1       |    3    |    1        |
            |    2       |    4    |    2        |
            |    3       |    3    |    1        |
            |    4       |    5    |    2        |
            |    5       |    3    |    1        |
            |    6       |    4    |    2        |
            |    7       |    3    |    1        |
            ================ END DIRECTORY ================
            */

            assert_eq!(directory_page.can_shrink(), true);
            directory_page.decr_global_depth();

            /*
            ======== DIRECTORY (global_depth_: 2) ========
            | bucket_idx | page_id | local_depth |
            |    0       |    5    |    2        |
            |    1       |    3    |    1        |
            |    2       |    4    |    2        |
            |    3       |    3    |    1        |
            ================ END DIRECTORY ================
            */

            directory_page.verify_integrity();
            assert_eq!(directory_page.size(), 4);
            assert_eq!(directory_page.can_shrink(), false);
        } // page guard dropped
    }
}
