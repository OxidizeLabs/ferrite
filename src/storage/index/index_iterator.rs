use crate::common::rid::RID;
use crate::storage::index::b_plus_tree_i::BPlusTree;
use crate::storage::table::tuple::Tuple;
use log::{debug, error};
use std::sync::Arc;
use parking_lot::RwLock;

/// An iterator over entries in a B+ tree index.
#[derive(Debug)]
pub struct IndexIterator {
    tree: Arc<RwLock<BPlusTree>>,
    results: Vec<(Tuple, RID)>,
    position: usize,
    start_key: Option<Tuple>,
    end_key: Option<Tuple>,
    batch_size: usize,
    exhausted: bool,
    prev_batch_last_key: Option<Tuple>,  // Track last key from previous batch
}

impl IndexIterator {
    /// Create a new index iterator with batched scanning.
    pub fn new(
        tree: Arc<RwLock<BPlusTree>>,
        start_key: Option<Tuple>,
        end_key: Option<Tuple>,
    ) -> Self {
        println!("Creating new IndexIterator");
        println!("Start key: {:?}", start_key.as_ref().map(|k| k.get_value(0)));
        println!("End key: {:?}", end_key.as_ref().map(|k| k.get_value(0)));

        let batch_size = 1000;
        let mut results: Vec<(_, _)> = Vec::with_capacity(batch_size);

        // Initialize first batch if we have bounds
        if let (Some(start), Some(end)) = (&start_key, &end_key) {
            let tree_guard = tree.read();
            tree_guard.scan_range(start, end, true, &mut results);
            println!("Initial scan returned {} results", results.len());
        }

        Self {
            tree,
            results,
            position: 0,
            start_key,
            end_key,
            batch_size,
            exhausted: false,
            prev_batch_last_key: None,
        }
    }

    /// Fetch next batch of results
    fn fetch_next_batch(&mut self) -> bool {
        if self.exhausted {
            println!("Iterator already exhausted");
            return false;
        }

        println!("\nFETCHING BATCH:");
        println!("- Current position: {}", self.position);
        println!("- Current results length: {}", self.results.len());

        match (&self.start_key, &self.end_key) {
            (Some(start), Some(end)) => {
                let tree_guard = self.tree.read();

                let start_key = if self.position == 0 && self.results.is_empty() {
                    println!("- Using initial start key");
                    start.clone()
                } else if !self.results.is_empty() && self.position > 0 {
                    println!("- Using key from current results");
                    let prev_key = &self.results[self.position - 1].0;
                    println!("- Previous key value: {:?}", prev_key.get_value(0));

                    // Check if we've reached the end
                    let cmp = tree_guard.compare_keys(prev_key, end);
                    if cmp.is_eq() {
                        println!("- Reached end key");
                        self.exhausted = true;
                        return false;
                    } else if cmp.is_gt() {
                        println!("- Passed end key");
                        self.exhausted = true;
                        return false;
                    }

                    prev_key.clone()
                } else {
                    println!("- No valid start key found");
                    self.exhausted = true;
                    return false;
                };

                // Always include end for potential last batch
                println!("- Starting scan from {:?} to {:?} (include_end: true)",
                         start_key.get_value(0), end.get_value(0));

                self.results.clear();
                tree_guard.scan_range(&start_key, end, true, &mut self.results);
                println!("- Scan returned {} results", self.results.len());

                if !self.results.is_empty() {
                    println!("- First result key: {:?}", self.results[0].0.get_value(0));
                    println!("- Last result key: {:?}",
                             self.results.last().unwrap().0.get_value(0));
                }

                // Remove duplicate at boundary except if it's the end key
                if self.position > 0 && !self.results.is_empty() {
                    let first_key = &self.results[0].0;
                    let cmp_start = tree_guard.compare_keys(first_key, &start_key);
                    let cmp_end = tree_guard.compare_keys(first_key, end);

                    if cmp_start.is_eq() && !cmp_end.is_eq() {
                        println!("- Removing duplicate first item");
                        self.results.remove(0);
                    }
                }

                self.position = 0;

                if self.results.is_empty() {
                    println!("- Got empty batch, marking exhausted");
                    self.exhausted = true;
                    false
                } else {
                    !self.results.is_empty()
                }
            }
            _ => {
                println!("Missing bounds for batch fetch");
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
        if self.position >= self.results.len() {
            if self.exhausted || !self.fetch_next_batch() {
                debug!("No more results available");
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
                tree_guard.scan_range(seek_key, end, true, &mut self.results);
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

        // Insert test data in a separate scope
        {
            let tree_guard = tree.write();
            for i in 1..=5 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                tree_guard.insert(tuple, RID::new(0, i as u32));
            }
        } // write lock is released here

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

        // Insert data in a separate scope
        {
            let tree_guard = tree.write();
            for i in 1..=2000 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                tree_guard.insert(tuple, RID::new(0, i as u32));
            }
        } // write lock is released here

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

    #[test]
    fn test_iterator_empty_range() {
        let schema = create_test_schema();
        let tree = create_test_tree(4);

        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(10, "value10", &schema);

        let iterator = IndexIterator::new(
            Arc::clone(&tree),
            Some(start),
            Some(end)
        );

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
            let tree_guard = tree.write();
            for i in 1..=10 {
                let tuple = create_tuple(i, &format!("value{}", i), &schema);
                assert!(tree_guard.insert(tuple, RID::new(0, i as u32)));
            }
        }

        let start = create_tuple(1, "value1", &schema);
        let end = create_tuple(10, "value10", &schema);

        // Use very small batch size to force multiple batches
        let mut iterator = IndexIterator::new(Arc::clone(&tree), Some(start), Some(end));
        iterator.batch_size = 3;  // Force small batches

        // Collect all results and verify
        let results: Vec<RID> = iterator.collect();
        println!("Got {} results: {:?}", results.len(), results);

        // Verify we got all expected RIDs
        let expected: Vec<RID> = (1..=10).map(|i| RID::new(0, i)).collect();
        assert_eq!(results, expected, "Results don't match expected values");
    }
}