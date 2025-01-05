use crate::common::rid::RID;
use crate::storage::index::b_plus_tree_i::BPlusTree;
use crate::storage::table::tuple::Tuple;
use log::{debug, error};
use std::sync::Arc;
use parking_lot::RwLock;

/// An iterator over entries in a B+ tree index.
#[derive(Debug)]
pub struct IndexIterator {
    /// Reference to the B+ tree being iterated
    tree: Arc<RwLock<BPlusTree>>,
    /// Current results from tree scan
    results: Vec<(Tuple, RID)>,
    /// Current position in results
    position: usize,
    /// Start key for the scan
    start_key: Option<Tuple>,
    /// End key for the scan
    end_key: Option<Tuple>,
    /// Batch size for fetching results
    batch_size: usize,
    /// Whether we've reached the end of all possible results
    exhausted: bool,
}

impl IndexIterator {
    /// Create a new index iterator with batched scanning.
    pub fn new(
        tree: Arc<RwLock<BPlusTree>>,
        start_key: Option<Tuple>,
        end_key: Option<Tuple>,
    ) -> Self {
        debug!("Creating new IndexIterator with batched scanning");

        let batch_size = 1000; // Configurable batch size
        let mut results = Vec::with_capacity(batch_size);

        // Initialize first batch if we have bounds
        if let (Some(start), Some(end)) = (&start_key, &end_key) {
            let tree_guard = tree.read();
            debug!("Initial batch scan from {:?} to {:?}",
                start.get_value(0), end.get_value(0));
            tree_guard.scan_range(start, end, &mut results);
        }

        Self {
            tree,
            results,
            position: 0,
            start_key,
            end_key,
            batch_size,
            exhausted: false,
        }
    }

    /// Fetch next batch of results
    fn fetch_next_batch(&mut self) -> bool {
        if self.exhausted {
            return false;
        }

        // Clear existing results and reuse allocation
        self.results.clear();

        match (&self.start_key, &self.end_key) {
            (Some(start), Some(end)) => {
                let tree_guard = self.tree.read();

                // Use last key from previous batch as new start
                let new_start = if self.position > 0 {
                    self.results.last().map(|(tuple, _)| tuple.clone())
                } else {
                    Some(start.clone())
                };

                if let Some(start_key) = new_start {
                    debug!("Fetching next batch from {:?}", start_key.get_value(0));
                    tree_guard.scan_range(&start_key, end, &mut self.results);

                    // If we got fewer results than batch size, we're done
                    if self.results.len() < self.batch_size {
                        self.exhausted = true;
                    }

                    !self.results.is_empty()
                } else {
                    self.exhausted = true;
                    false
                }
            }
            _ => {
                error!("Cannot fetch batch without proper bounds");
                self.exhausted = true;
                false
            }
        }
    }

    /// Check if we've reached the end of the current batch
    fn is_batch_end(&self) -> bool {
        self.position >= self.results.len()
    }

    /// Get current tuple and RID
    fn get_current(&self) -> Option<(Tuple, RID)> {
        if self.position < self.results.len() {
            Some(self.results[self.position].clone())
        } else {
            None
        }
    }
}

impl Iterator for IndexIterator {
    type Item = RID;

    fn next(&mut self) -> Option<Self::Item> {
        // Check if we need to fetch next batch
        if self.is_batch_end() && !self.exhausted {
            if !self.fetch_next_batch() {
                debug!("No more results available");
                return None;
            }
            self.position = 0;
        }

        // Get current tuple and RID
        if let Some((_, rid)) = self.get_current() {
            debug!("Returning RID: {:?}", rid);
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
                tree_guard.scan_range(seek_key, end, &mut self.results);
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
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
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

    pub fn create_test_metadata(
        table_name: String,
        index_name: String,
    ) -> IndexInfo {
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
    use super::test_utils::*;
    use super::*;

    #[test]
    fn test_iterator_basic() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Insert test data
        let tree_guard = tree.write();
        for i in 1..=5 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree_guard.insert(tuple, RID::new(0, i as u32));
        }

        // Test range scan
        let start = create_tuple(2, "value2", &schema);
        let end = create_tuple(4, "value4", &schema);

        let iterator = IndexIterator::new(
            Arc::clone(&tree),
            Some(start),
            Some(end)
        );

        let results: Vec<RID> = iterator.collect();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_iterator_batching() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        // Insert more data to test batching
        let tree_guard = tree.write();
        for i in 1..=2000 {
            let tuple = create_tuple(i, &format!("value{}", i), &schema);
            tree_guard.insert(tuple, RID::new(0, i as u32));
        }

        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(2000, "value2000", &schema);

        let iterator = IndexIterator::new(
            Arc::clone(&tree),
            Some(start),
            Some(end)
        );

        let results: Vec<RID> = iterator.collect();
        assert_eq!(results.len(), 2000);
    }
}