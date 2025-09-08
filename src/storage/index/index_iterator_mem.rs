use crate::common::rid::RID;
use crate::storage::index::b_plus_tree::BPlusTree;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use log::debug;
use parking_lot::RwLock;
use std::sync::Arc;

/// An iterator over entries in a B+ tree index.
#[derive(Debug)]
pub struct IndexIterator {
    tree: Arc<RwLock<BPlusTree>>,
    current_batch: Vec<(Value, RID)>,
    position: usize,
    start_key: Option<Arc<Tuple>>,
    end_key: Option<Arc<Tuple>>,
    exhausted: bool,
    last_key: Option<Value>, // Track the last key we've processed
}

impl IndexIterator {
    /// Create a new index iterator with batched scanning.
    pub fn new(
        tree: Arc<RwLock<BPlusTree>>,
        start_key: Option<Arc<Tuple>>,
        end_key: Option<Arc<Tuple>>,
    ) -> Self {
        let mut iterator = Self {
            tree,
            current_batch: Vec::new(), // Don't pre-allocate since we'll replace it
            position: 0,
            start_key,
            end_key,
            exhausted: false,
            last_key: None,
        };

        // Fetch first batch
        iterator.fetch_next_batch();
        iterator
    }

    /// Get current RID
    pub fn get_rid(&self) -> Option<RID> {
        self.current_batch.get(self.position).map(|(_, rid)| *rid)
    }

    /// Check if iterator has reached the end
    pub fn is_end(&self) -> bool {
        self.exhausted && self.position >= self.current_batch.len()
    }

    /// Fetch next batch of results
    fn fetch_next_batch(&mut self) -> bool {
        if self.exhausted {
            debug!("Iterator already exhausted");
            return false;
        }

        debug!("Fetching next batch");
        self.current_batch.clear();
        self.position = 0;

        let tree_guard = self.tree.read();
        let result = match (&self.start_key, &self.end_key) {
            // Full scan case
            (None, None) => {
                if self.last_key.is_some() {
                    debug!("Already completed full scan");
                    self.exhausted = true;
                    return false;
                }
                debug!("Starting fresh full scan");
                tree_guard.scan_full()
            }

            // Range scan case
            (Some(start), Some(end)) => {
                let scan_start = if let Some(last_key) = &self.last_key {
                    let key_attrs = tree_guard.get_metadata().get_key_attrs().clone();
                    let schema = start.get_schema();
                    let mut values = start.get_values();

                    // Only modify the first key (the one we're iterating on)
                    for &idx in &key_attrs {
                        if idx == 0 {
                            // Assuming we're always iterating on the first column
                            values[idx] = last_key.clone();
                        }
                    }

                    // Create a new tuple with the modified values
                    let new_start = Arc::new(Tuple::new(&values, &schema, RID::new(0, 0)));
                    debug!("Continuing range scan from key: {:?}", new_start);
                    new_start
                } else {
                    debug!("Starting fresh range scan from: {:?}", start);
                    start.clone()
                };

                tree_guard.scan_range(&scan_start, end, self.last_key.is_none())
            }
            _ => {
                debug!("Invalid scan range configuration");
                Ok(Vec::new())
            }
        };

        match result {
            Ok(new_results) => {
                if new_results.is_empty() {
                    debug!("No more results available");
                    self.exhausted = true;
                    return false;
                }

                // Update last_key for next batch
                if let Some((key, _)) = new_results.last() {
                    self.last_key = Some(key.clone());
                }

                self.current_batch = new_results;
                debug!("Fetched {} results", self.current_batch.len());
                true
            }
            Err(e) => {
                debug!("Error fetching batch: {:?}", e);
                self.exhausted = true;
                false
            }
        }
    }

    /// Reset the iterator to start of index
    pub fn reset(&mut self) {
        self.current_batch.clear();
        self.position = 0;
        self.exhausted = false;
        self.fetch_next_batch();
    }
}

impl Iterator for IndexIterator {
    type Item = RID;

    fn next(&mut self) -> Option<Self::Item> {
        if self.is_end() {
            debug!("Iterator has reached the end");
            return None;
        }

        // If we've exhausted current batch, try to fetch next batch
        if self.position >= self.current_batch.len() {
            if !self.fetch_next_batch() {
                debug!("No more batches available");
                return None;
            }
            // Reset position for new batch
            self.position = 0;
        }

        // Return current item and advance position
        if self.position < self.current_batch.len() {
            let rid = self.current_batch[self.position].1;
            debug!("Returning RID {:?} at position {}", rid, self.position);
            self.position += 1;
            Some(rid)
        } else {
            debug!("No more items in current batch");
            None
        }
    }
}

// Add support for reverse iteration
impl DoubleEndedIterator for IndexIterator {
    fn next_back(&mut self) -> Option<Self::Item> {
        if self.current_batch.is_empty() {
            return None;
        }

        if let Some((_, rid)) = self.current_batch.pop() {
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
        self.current_batch.clear();
        self.position = 0;
        self.exhausted = false;

        // Perform new scan from seek key in its own scope
        {
            let tree_guard = self.tree.read();
            if let Some(end) = &self.end_key {
                self.current_batch.append(
                    tree_guard
                        .scan_range(seek_key, end, true)
                        .unwrap()
                        .as_mut(),
                );
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
            self.current_batch.len() - self.position
        }
    }
}

#[cfg(test)]
mod test_utils {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::concurrency::transaction::{IsolationLevel, Transaction};
    use crate::storage::index::{IndexInfo, IndexType};
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

    pub fn create_tuple(id: i32, value: &str, schema: &Schema) -> Arc<Tuple> {
        let values = vec![Value::new(id), Value::new(value)];
        Arc::new(Tuple::new(&values, schema, RID::new(0, 0)))
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

    pub fn create_text_txn() -> Transaction {
        Transaction::new(0, IsolationLevel::ReadCommitted)
    }
}

#[cfg(test)]
mod tests {
    use super::test_utils::*;
    use super::*;
    use crate::common::logger::initialize_logger;
    use crate::storage::index::Index;

    #[test]
    fn test_iterator_basic() {
        initialize_logger();
        let schema = create_test_schema();
        let tree = create_test_tree(4);
        let txn = create_text_txn();

        // Insert test data in a separate scope
        {
            let mut tree_guard = tree.write();
            for i in 1..=5 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &txn);
            }
        } // write lock is released here

        // Test range scan
        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);

        let iterator = IndexIterator::new(Arc::clone(&tree), Some(start), Some(end));

        let results: Vec<RID> = iterator.collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_iterator_batching() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);
        let txn = create_text_txn();

        // Insert data in a separate scope
        {
            let mut tree_guard = tree.write();
            for i in 1..=200 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                tree_guard.insert_entry(&tuple, RID::new(0, i as u32), &txn);
            }
        } // write lock is released here

        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(200, "value200", &schema);

        let iterator = IndexIterator::new(Arc::clone(&tree), Some(start), Some(end));

        let results: Vec<RID> = iterator.collect();
        assert_eq!(results.len(), 200);
    }

    #[test]
    fn test_iterator_empty_range() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(10, "value10", &schema);

        let iterator = IndexIterator::new(Arc::clone(&tree), Some(start), Some(end));

        let results: Vec<RID> = iterator.collect();
        assert!(results.is_empty());
    }
}
