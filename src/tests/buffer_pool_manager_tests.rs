use crate::buffer::buffer_pool_manager::BufferPoolManager;
use crate::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use crate::common::config::DB_PAGE_SIZE;
use crate::disk::disk_manager::DiskManager;
use rand::Rng;
use std::sync::{Arc, Mutex};

#[test]
fn binary_data_test() {
    let db_name = "test.db";
    let db_log = "test.log";
    let buffer_pool_size = 10;
    let disk_manager = Arc::new(DiskManager::new(db_name, db_log));
    let replacer = Arc::new(Mutex::new(LRUKReplacer::new(7, 2)));
    let bpm = BufferPoolManager::new(buffer_pool_size, disk_manager.clone(), replacer);

    let page_id_temp = 0;
    let page0 = bpm.new_page();

    // Scenario: The buffer pool is empty. We should be able to create a new page.
    assert!(page0.is_some());
    assert_eq!(0, page_id_temp);

    let mut rng = rand::thread_rng();
    let mut random_binary_data = [0u8; DB_PAGE_SIZE];
    rng.fill(&mut random_binary_data[..]);

    // Insert terminal characters both in the middle and at end
    random_binary_data[DB_PAGE_SIZE / 2] = 0;
    random_binary_data[DB_PAGE_SIZE - 1] = 0;

    // Scenario: Once we have a page, we should be able to read and write content.
    {
        let mut page0 = &page0.unwrap();
        page0.get_data().copy_from_slice(&random_binary_data);
        assert_eq!(random_binary_data, page0.get_data());
    }

    // Scenario: We should be able to create new pages until we fill up the buffer pool.
    for _ in 1..buffer_pool_size {
        assert!(bpm.new_page().is_some());
    }

    // Scenario: Once the buffer pool is full, we should not be able to create any new pages.
    for _ in buffer_pool_size..(buffer_pool_size * 2) {
        assert!(bpm.new_page().is_none());
    }

    // Scenario: After unpinning pages {0, 1, 2, 3, 4}, we should be able to create 5 new pages
    for i in 0..5 {
        assert!(bpm.unpin_page(i, true, AccessType::Lookup));
        bpm.flush_page(i);
    }
    for _ in 0..5 {
        assert!(bpm.new_page().is_some());
        bpm.unpin_page(page_id_temp, false, AccessType::Lookup);
    }

    // Scenario: We should be able to fetch the data we wrote a while ago.
    let page0 = bpm.fetch_page(0, AccessType::Lookup);
    // assert!(page0.clone().type_id());
    let binding = page0.clone();
    let page0 = page0.read().unwrap();
    assert_eq!(random_binary_data, page0.get_data());

    bpm.unpin_page(0, true, AccessType::Lookup);

    // Shutdown the disk manager and remove the temporary file we created.
    &disk_manager.shut_down();
    std::fs::remove_file("test.db").unwrap();
}
