use log::{debug, info};
use spin::RwLock;
use std::sync::Arc;
use tkdb::storage::index::b_plus_tree_i::BPlusTree;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_setup::initialize_logger;

    // #[test]
    // fn test_insert_and_search() {
    //     initialize_logger();
    //     let bpt: BPlusTree<i32, &str> = BPlusTree::new(4);
    //
    //     bpt.insert(1, "value1");
    //     bpt.insert(2, "value2");
    //     bpt.insert(3, "value3");
    //     bpt.insert(4, "value4");
    //
    //     assert_eq!(bpt.search(&1), Some("value1"));
    //     assert_eq!(bpt.search(&2), Some("value2"));
    //     assert_eq!(bpt.search(&3), Some("value3"));
    //     assert_eq!(bpt.search(&4), Some("value4"));
    //     assert_eq!(bpt.search(&5), None);
    // }
    //
    // #[test]
    // fn test_delete_leaf_node() {
    //     initialize_logger();
    //     let mut bpt: BPlusTree<i32, &str> = BPlusTree::new(4);
    //
    //     bpt.insert(1, "value1");
    //     bpt.insert(2, "value2");
    //     bpt.insert(3, "value3");
    //     bpt.insert(4, "value4");
    //
    //     bpt.delete(&2);
    //     assert_eq!(bpt.search(&2), None);
    //     assert_eq!(bpt.search(&1), Some("value1"));
    //     assert_eq!(bpt.search(&3), Some("value3"));
    //     assert_eq!(bpt.search(&4), Some("value4"));
    //
    //     bpt.delete(&1);
    //     assert_eq!(bpt.search(&1), None);
    // }

    #[test]
    fn test_delete_internal_node() {
        initialize_logger();
        let mut bpt: BPlusTree<i32, &str> = BPlusTree::new(4);

        bpt.insert(1, "value1");
        bpt.insert(2, "value2");
        bpt.insert(3, "value3");
        bpt.insert(4, "value4");
        bpt.insert(5, "value5");

        debug!("{}", bpt.to_string());

        bpt.delete(&3);

        debug!("{}", bpt.to_string());
        assert_eq!(bpt.search(&3), None);

        debug!("{}", bpt.to_string());
        assert_eq!(bpt.search(&2), Some("value2"));
        assert_eq!(bpt.search(&4), Some("value4"));
    }

    #[test]
    fn test_edge_case_deletion() {
        initialize_logger();
        let mut bpt: BPlusTree<i32, &str> = BPlusTree::new(4);

        bpt.insert(1, "value1");
        bpt.insert(2, "value2");

        bpt.delete(&1);
        assert_eq!(bpt.search(&1), None);
        assert_eq!(bpt.search(&2), Some("value2"));

        bpt.delete(&2);
        assert_eq!(bpt.search(&2), None);

        // Ensure tree is still functional after deletions
        bpt.insert(3, "value3");
        assert_eq!(bpt.search(&3), Some("value3"));
    }

    #[test]
    fn test_to_string() {
        initialize_logger();
        let bpt: BPlusTree<i32, &str> = BPlusTree::new(4);

        bpt.insert(1, "value1");
        bpt.insert(2, "value2");
        bpt.insert(3, "value3");
        bpt.insert(4, "value4");

        let tree_string = bpt.to_string();
        assert!(tree_string.contains("Internal Node: [3]"));
        assert!(tree_string.contains("Leaf Node: [1, 2]"));
        assert!(tree_string.contains("Leaf Node: [3, 4]"));
    }

    #[test]
    fn test_split_child() {
        initialize_logger();
        let bpt: BPlusTree<i32, &str> = BPlusTree::new(4);

        bpt.insert(1, "value1");
        bpt.insert(2, "value2");
        bpt.insert(3, "value3");
        bpt.insert(4, "value4");
        bpt.insert(5, "value5");

        let tree_string = bpt.to_string();
        assert!(tree_string.contains("Internal Node: [3]"));
        assert!(tree_string.contains("Leaf Node: [1, 2]"));
        assert!(tree_string.contains("Leaf Node: [3, 4, 5]"));
    }

    #[test]
    fn basic_operations() {
        initialize_logger();
        let bpt = Arc::new(RwLock::new(BPlusTree::new(4)));

        {
            let mut bpt_guard = bpt.write();
            info!("Inserting key: 1, value: \"value1\"");
            bpt_guard.insert(1, "value1".to_string());
            info!("Inserting key: 2, value: \"value2\"");
            bpt_guard.insert(2, "value2".to_string());
            info!("Inserting key: 3, value: \"value3\"");
            bpt_guard.insert(3, "value3".to_string());
            info!("Inserting key: 4, value: \"value4\"");
            bpt_guard.insert(4, "value4".to_string());

            debug!("{}", bpt_guard.to_string());

            info!("Searching for key: 1");
            assert_eq!(bpt_guard.search(&1), Some("value1".to_string()));
            info!("Searching for key: 2");
            assert_eq!(bpt_guard.search(&2), Some("value2".to_string()));
            info!("Searching for key: 3");
            assert_eq!(bpt_guard.search(&3), Some("value3".to_string()));
            info!("Searching for key: 4");
            assert_eq!(bpt_guard.search(&4), Some("value4".to_string()));

            info!("Deleting key: 2");
            bpt_guard.delete(&2);
            debug!("{}", bpt_guard.to_string());
            info!("Searching for key: 2");
            assert_eq!(bpt_guard.search(&2), None);

            info!("Deleting key: 1");
            bpt_guard.delete(&1);
            debug!("{}", bpt_guard.to_string());
            info!("Searching for key: 1");
            assert_eq!(bpt_guard.search(&1), None);

            info!("Deleting key: 3");
            bpt_guard.delete(&3);
            debug!("{}", bpt_guard.to_string());
            info!("Searching for key: 3");
            assert_eq!(bpt_guard.search(&3), None);

            info!("Deleting key: 4");
            bpt_guard.delete(&4);
            debug!("{}", bpt_guard.to_string());
            info!("Searching for key: 4");
            assert_eq!(bpt_guard.search(&4), None);
        }
    }

    // #[tokio::test]
    // async fn concurrent_operations() {
    //     initialize_logger();
    //
    //     let bpt = Arc::new(RwLock::new(BPlusTree::new(4)));
    //
    //     let handles: Vec<_> = (0..10)
    //         .map(|i| {
    //             let bpt_clone = Arc::clone(&bpt);
    //             task::spawn(async move {
    //                 for j in 0..10 {
    //                     let key = i * 10 + j;
    //                     let value = format!("value{}", key);
    //                     info!("Inserting key: {}, value: {}", key, value);
    //                     bpt_clone.write().insert(key, value);
    //                 }
    //             })
    //         })
    //         .collect();
    //
    //     for handle in handles {
    //         handle.await.unwrap();
    //     }
    //
    //     let bpt_guard = bpt.read();
    //     debug!("{}", bpt_guard.to_string());
    //
    //     for i in 0..10 {
    //         for j in 0..10 {
    //             let key = i * 10 + j;
    //             let expected_value = format!("value{}", key);
    //             info!("Searching for key: {}", key);
    //             assert_eq!(bpt_guard.search(&key), Some(expected_value));
    //         }
    //     }
    //
    //     let delete_handles: Vec<_> = (0..10)
    //         .map(|i| {
    //             let bpt_clone = Arc::clone(&bpt);
    //             task::spawn(async move {
    //                 for j in 0..10 {
    //                     let key = i * 10 + j;
    //                     info!("Deleting key: {}", key);
    //                     bpt_clone.write().delete(&key);
    //                 }
    //             })
    //         })
    //         .collect();
    //
    //     for handle in delete_handles {
    //         handle.await.unwrap();
    //     }
    //
    //     let bpt_guard = bpt.read();
    //     debug!("{}", bpt_guard.to_string());
    //
    //     for i in 0..10 {
    //         for j in 0..10 {
    //             let key = i * 10 + j;
    //             info!("Searching for key: {}", key);
    //             assert_eq!(bpt_guard.search(&key), None);
    //         }
    //     }
    // }
}
