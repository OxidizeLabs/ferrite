use crate::common::rid::RID;
use crate::storage::index::b_plus_tree_i::BPlusTree;
use crate::storage::table::tuple::Tuple;
use log::debug;
use parking_lot::RwLock;
use std::sync::Arc;
use crate::types_db::value::Value;

/// An iterator over entries in a B+ tree index.
#[derive(Debug)]
pub struct IndexIterator {
    tree: Arc<RwLock<BPlusTree>>,
    results: Vec<(Value, RID)>,
    position: usize,
    start_key: Option<Tuple>,
    end_key: Option<Tuple>,
    batch_size: usize,
    exhausted: bool,
}

impl IndexIterator {
    /// Create a new index iterator with batched scanning.
    pub fn new(
        tree: Arc<RwLock<BPlusTree>>,
        batch_size: usize,
        start_key: Option<Tuple>,
        end_key: Option<Tuple>,
    ) -> Self {
        let mut iterator = Self {
            tree,
            results: Vec::with_capacity(batch_size),
            position: 0,
            start_key,
            end_key,
            batch_size,
            exhausted: false,
        };

        // Fetch first batch
        iterator.fetch_next_batch();
        iterator
    }

    /// Fetch next batch of results
    fn fetch_next_batch(&mut self) -> bool {
        if self.exhausted {
            debug!("Iterator already exhausted");
            return false;
        }

        debug!("Fetching next batch, current position: {}", self.position);
        self.results.clear();

        let tree_guard = self.tree.read();
        let result = match (&self.start_key, &self.end_key) {
            // Full scan case
            (None, None) => {
                let empty_key = tree_guard.get_metadata().create_dummy_key();
                tree_guard.scan_range(&empty_key, &empty_key, true)
            }
            // Range scan case
            (Some(start), Some(end)) => {
                // If we're not at the beginning, use the last key as the new start
                let scan_start = if self.position > 0 && !self.results.is_empty() {
                    let last_key = &self.results[self.position - 1].0;
                    let mut new_start = start.clone();
                    new_start.keys_from_tuple(tree_guard.get_metadata().get_key_attrs().clone())[0] = last_key.clone();
                    new_start
                } else {
                    start.clone()
                };

                tree_guard.scan_range(&scan_start, end, true)
            }
            // Invalid cases
            _ => Ok(Vec::new()),
        };

        match result {
            Ok(mut new_results) => {
                // For range scans, remove the first item if it's a duplicate
                if self.position > 0 && !self.results.is_empty() && !new_results.is_empty() {
                    if let Some(last_key) = self.results.last().map(|(k, _)| k) {
                        if tree_guard.compare_keys_ordering(last_key, &new_results[0].0).is_eq() {
                            new_results.remove(0);
                        }
                    }
                }

                // Take only batch_size items
                let take_count = self.batch_size.min(new_results.len());
                self.results = new_results.into_iter().take(take_count).collect();

                self.position = 0;

                if self.results.is_empty() {
                    debug!("No more results available");
                    self.exhausted = true;
                    false
                } else {
                    debug!("Fetched {} results", self.results.len());
                    true
                }
            }
            Err(e) => {
                debug!("Error fetching batch: {}", e);
                self.exhausted = true;
                false
            }
        }
    }
}

impl Iterator for IndexIterator {
    type Item = RID;

    fn next(&mut self) -> Option<Self::Item> {
        // Check if we need to fetch next batch
        if self.position >= self.results.len() {
            if !self.fetch_next_batch() {
                return None;
            }
        }

        // Get current result
        if self.position < self.results.len() {
            let rid = self.results[self.position].1;
            debug!("Returning RID {:?} at position {}", rid, self.position);
            self.position += 1;
            Some(rid)
        } else {
            debug!("No more items available");
            None
        }
    }
}

// Add support for reverse iteration
impl DoubleEndedIterator for IndexIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.results.is_empty() {
            return None;
        }

        if let Some((_, rid)) = self.results.pop() {
            Some(rid)
        } else {
            None
        }
    }
}

// Implement additional helper methods for the iterator
impl IndexIterator {
    /// Skip to a specific key in the range
    pub fn seek(&mut self, seek_key: &Tuple) -> Option<RID> {
        // Reset iterator state
        self.results.clear();
        self.position = 0;
        self.exhausted = false;

        // Perform new scan from seek key in its own scope
        {
            let tree_guard = self.tree.read();
            if let Some(end) = &self.end_key {
                self.results.append(tree_guard.scan_range(&seek_key, &end, true).unwrap().as_mut());
            }
        } // tree_guard is dropped here

        // Now we can safely call next()
        self.next()
    }

    /// Get an estimate of remaining items
    pub fn estimate_remaining(&self) -> usize {
        if self.exhausted {
            0
        } else {
            // Rough estimate based on current batch
            self.results.len() - self.position
        }
    }
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::storage::index::index::{IndexInfo, IndexType};
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    pub fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::VarChar),
        ])
    }

    pub fn create_key_schema() -> Schema {
        // Key schema only includes the 'id' column
        Schema::new(vec![Column::new("id", TypeId::Integer)])
    }

    pub fn create_tuple(id: i32, value: &str, schema: &Schema) -> Tuple {
        let values = vec![Value::new(id), Value::new(value)];
        Tuple::new(&values, schema.clone(), RID::new(0, 0))
    }

    pub fn create_test_metadata(table_name: String, index_name: String) -> IndexInfo {
        IndexInfo::new(
            create_key_schema(),
            index_name,
            0,
            table_name,
            4,
            false,
            IndexType::BPlusTreeIndex,
            vec![0], // Index on 'id' column
        )
    }

    pub fn create_test_tree(order: usize) -> Arc<RwLock<BPlusTree>> {
        let metadata = create_test_metadata("test_table".to_string(), "test_index".to_string());
        Arc::new(RwLock::new(BPlusTree::new(order, Arc::from(metadata))))
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::index::index::Index;
    use super::test_utils::*;
    use super::*;

    #[test]
    fn test_iterator_basic() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Insert test data in a separate scope
        {
            let mut tree_guard = tree.write();
            for i in 1..=5 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default());
            }
        } // write lock is released here

        // Test range scan
        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);

        let iterator = IndexIterator::new(Arc::clone(&tree), 1, Some(start), Some(end));

        let results: Vec<RID> = iterator.collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_iterator_batching() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Insert data in a separate scope
        {
            let mut tree_guard = tree.write();
            for i in 1..=2000 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default());
            }
        } // write lock is released here

        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(2000, "value2000", &schema);

        let iterator = IndexIterator::new(Arc::clone(&tree), 10, Some(start), Some(end));

        let results: Vec<RID> = iterator.collect();
        assert_eq!(results.len(), 2000);
    }

    #[test]
    fn test_iterator_empty_range() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(10, "value10", &schema);

        let iterator = IndexIterator::new(Arc::clone(&tree), 1, Some(start), Some(end));

        let results: Vec<RID> = iterator.collect();
        assert!(results.is_empty());
    }

    #[test]
    fn test_iterator_batch_boundaries() {
        use crate::common::logger::initialize_logger;
        initialize_logger();

        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Insert a small set of data to test batch boundaries
        {
            let mut tree_guard = tree.write();
            for i in 1..=10 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                assert!(tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &Default::default()));
            }
        }

        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(10, "value10", &schema);

        // Use very small batch size to force multiple batches
        let iterator = IndexIterator::new(Arc::clone(&tree), 3, Some(start), Some(end));

        // Collect all results and verify
        let results: Vec<RID> = iterator.collect();
        println!("Got {} results: {:?}", results.len(), results);

        // Verify we got all expected RIDs
        let expected: Vec<RID> = (1..=10).map(|i| RID::new(0, i)).collect();
        assert_eq!(results, expected, "Results don't match expected values");
    }
}
