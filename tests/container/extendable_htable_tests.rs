use chrono::Utc;
use std::sync::Arc;
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::container::disk_extendable_hash_table::DiskExtendableHashTable;
use tkdb::container::hash_function::HashFunction;
use tkdb::storage::disk::disk_manager::FileDiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use tkdb::storage::index::int_comparator::IntComparator;
use tokio::fs::remove_file;
use tokio::sync::Mutex;

use crate::test_setup::initialize_logger;

struct TestContext {
    bpm: Arc<BufferPoolManager>,
    db_file: String,
    db_log_file: String,
}

impl TestContext {
    async fn new(test_name: &str) -> Self {
        initialize_logger();
        let buffer_pool_size: usize = 5;
        const K: usize = 2;
        let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
        let db_file = format!("{}_{}.db", test_name, timestamp);
        let db_log_file = format!("{}_{}.log", test_name, timestamp);
        let disk_manager =
            Arc::new(FileDiskManager::new(db_file.clone(), db_log_file.clone()).await);
        let disk_scheduler = Arc::new(Mutex::new(DiskScheduler::new(Arc::clone(
            &disk_manager,
        ))));
        let replacer = Arc::new(spin::Mutex::new(LRUKReplacer::new(buffer_pool_size, K)));
        let bpm = Arc::new(BufferPoolManager::new(
            buffer_pool_size,
            disk_scheduler,
            disk_manager.clone(),
            replacer.clone(),
        ));

        Self {
            bpm,
            db_file,
            db_log_file,
        }
    }

    async fn cleanup(&self) {
        let _ = remove_file(&self.db_file).await;
        let _ = remove_file(&self.db_log_file).await;
    }
}

impl Drop for TestContext {
    fn drop(&mut self) {
        let db_file = self.db_file.clone();
        let db_log_file = self.db_log_file.clone();
        let _ = std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                let _ = remove_file(db_file).await;
                let _ = remove_file(db_log_file).await;
            });
        }).join();
    }
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_insert1() {
        let ctx = TestContext::new("test_insert1").await;
        let bpm = &ctx.bpm;

        let ht = DiskExtendableHashTable::new(
            "blah".to_string(),
            bpm.clone(),
            IntComparator::new(),
            HashFunction::new(),
            2,
            2,
            0,
        ).await;

        let num_keys = 8;

        // insert some values
        for i in 0..num_keys {
            let inserted = ht.insert(&i, &i, None).await;
            assert!(inserted);
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(found);
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], i);
        }

        ht.verify_integrity().await;

        // attempt another insert, this should fail because table is full
        assert!(!ht.insert(&num_keys, &num_keys, None).await);
    }

    #[tokio::test]
    async fn test_insert2() {
        let ctx = TestContext::new("test_insert2").await;
        let bpm = &ctx.bpm;

        let ht = DiskExtendableHashTable::new(
            "blah".to_string(),
            bpm.clone(),
            IntComparator::new(),
            HashFunction::new(),
            2,
            2,
            0,
        ).await;

        let num_keys = 5;

        // insert some values
        for i in 0..num_keys {
            let inserted = ht.insert(&i, &i, None).await;
            assert!(inserted);
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(found);
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], i);
        }

        ht.verify_integrity().await;

        // check that they were actually inserted
        for i in 0..num_keys {
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(found);
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], i);
        }

        ht.verify_integrity().await;

        // try to get some keys that don't exist/were not inserted
        for i in num_keys..2 * num_keys {
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(!found);
            assert_eq!(res.len(), 0);
        }

        ht.verify_integrity().await;
    }

    #[tokio::test]
    async fn test_remove1() {
        let ctx = TestContext::new("test_remove1").await;
        let bpm = &ctx.bpm;

        let ht = DiskExtendableHashTable::new(
            "blah".to_string(),
            bpm.clone(),
            IntComparator::new(),
            HashFunction::new(),
            2,
            2,
            0,
        ).await;

        let num_keys = 5;

        // insert some values
        for i in 0..num_keys {
            let inserted = ht.insert(&i, &i, None).await;
            assert!(inserted);
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(found);
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], i);
        }

        ht.verify_integrity().await;

        // check that they were actually inserted
        for i in 0..num_keys {
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(found);
            assert_eq!(res.len(), 1);
            assert_eq!(res[0], i);
        }

        ht.verify_integrity().await;

        // try to get some keys that don't exist/were not inserted
        for i in num_keys..2 * num_keys {
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(!found);
            assert_eq!(res.len(), 0);
        }

        ht.verify_integrity().await;

        // remove the keys we inserted
        for i in 0..num_keys {
            let removed = ht.remove(&i, None).await;
            assert!(removed);
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(!found);
            assert_eq!(res.len(), 0);
        }

        ht.verify_integrity().await;

        // try to remove some keys that don't exist/were not inserted
        for i in num_keys..2 * num_keys {
            let removed = ht.remove(&i, None).await;
            assert!(!removed);
            let mut res = vec![];
            let found = ht.get_value(&i, &mut res, None).await;
            assert!(!found);
            assert_eq!(res.len(), 0);
        }

        ht.verify_integrity().await;
    }
}
