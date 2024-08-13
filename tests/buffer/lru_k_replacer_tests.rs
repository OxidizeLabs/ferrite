extern crate tkdb;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use tkdb::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use tkdb::common::time::TimeSource;

#[derive(Clone)]
pub struct MockTimeSource {
    time: Arc<Mutex<u64>>,
    increment: u64,
}

impl MockTimeSource {
    pub fn new() -> Self {
        Self {
            time: Arc::new(Mutex::new(0)),
            increment: 1000,
        }
    }

    pub fn advance_time(&self) {
        let mut time = self.time.lock().unwrap();
        *time += self.increment; // Increase time by a larger amount
    }

    pub fn get_time(&self) -> u64 {
        let time = self.time.lock().unwrap();
        *time
    }

    pub fn set_increment(&self, increment: u64) {
        let mut inc = self.time.lock().unwrap();
        *inc = increment;
    }
}

impl TimeSource for MockTimeSource {
    fn now(&self) -> u64 {
        self.get_time()
    }
}


#[cfg(test)]
mod tests {
    use std::thread::sleep;
    use super::*;

    #[test]
    fn basic_tests() {
        let ten_millis = time::Duration::from_millis(10);
        let mock_time = MockTimeSource::new();
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, Box::new(mock_time.clone()))));

        {
            let mut replacer = replacer.lock().unwrap();

            // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
            mock_time.advance_time();
            replacer.record_access(1, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(2, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(3, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(4, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(5, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(6, AccessType::Lookup);
            mock_time.advance_time();

            // Set all frames except 6 to be evictable
            replacer.set_evictable(1, true);
            replacer.set_evictable(2, true);
            replacer.set_evictable(3, true);
            replacer.set_evictable(4, true);
            replacer.set_evictable(5, true);
            replacer.set_evictable(6, false);

            assert_eq!(replacer.size(), 5, "Size should be 5 when 5 frames are evictable");
            assert_eq!(replacer.total_frames(), 6, "Total frames should be 6 after adding all frames");

            // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
            replacer.record_access(1, AccessType::Lookup);

            // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
            // first based on LRU.
            let mut value = replacer.evict().unwrap();
            assert_eq!(value, 2, "First evicted frame should be 2");
            value = replacer.evict().unwrap();
            assert_eq!(value, 3, "Second evicted frame should be 3");
            value = replacer.evict().unwrap();
            assert_eq!(value, 4, "Third evicted frame should be 4");
            assert_eq!(replacer.size(), 2, "Size should be 2 after three evictions");

            // Scenario: Now replacer has frames [5,1].
            // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
            replacer.record_access(3, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(4, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(5, AccessType::Lookup);
            mock_time.advance_time();
            replacer.set_evictable(3, true);
            mock_time.advance_time();
            replacer.set_evictable(4, true);
            mock_time.advance_time();

            assert_eq!(replacer.size(), 4, "Size should be 4 after adding two more evictable frames");

            // Scenario: continue looking for victims. We expect 3 to be evicted next.
            value = replacer.evict().unwrap();
            assert_eq!(value, 3, "Fourth evicted frame should be 3");
            assert_eq!(replacer.size(), 3, "Size should be 3 after one more eviction");

            // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
            replacer.set_evictable(6, true);
            assert_eq!(replacer.size(), 4, "Size should be 4 after making frame 6 evictable");
            value = replacer.evict().unwrap();
            assert_eq!(value, 6, "Fifth evicted frame should be 6");
            assert_eq!(replacer.size(), 3, "Size should be 3 after evicting frame 6");

            // Now we have [1,5,4]. Continue looking for victims.
            replacer.set_evictable(1, false);
            assert_eq!(replacer.size(), 2, "Size should be 2 after making frame 1 non-evictable");
            value = replacer.evict().unwrap();
            assert_eq!(value, 4, "Sixth evicted frame should be 4");
            assert_eq!(replacer.size(), 1, "Size should be 1 after evicting frame 4");

            // Update access history for 1. Now we have [5,1]. Next victim is 5.
            replacer.set_evictable(1, true);
            assert_eq!(replacer.size(), 2, "Size should be 2 after making frame 1 evictable again");
            value = replacer.evict().unwrap();
            assert_eq!(value, 5, "Seventh evicted frame should be 5");

            assert_eq!(replacer.size(), 1, "Size should be 1 after evicting frame 5");
            value = replacer.evict().unwrap();
            assert_eq!(value, 1, "Eighth evicted frame should be 1");
            assert_eq!(replacer.size(), 0, "Size should be 0 after all evictions");

            // This operation should not modify size
            assert!(replacer.evict().is_none(), "Eviction from an empty replacer should return None");
            assert_eq!(replacer.size(), 0, "Size should remain 0 after attempting to evict from empty replacer");
        }
    }

    #[test]
    fn test_edge_cases() {
        let mock_time_source = MockTimeSource::new();
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, Box::new(mock_time_source))));

        // Edge case: try to evict from an empty replacer
        assert!(replacer.lock().unwrap().evict().is_none(), "Eviction from an empty replacer should return None");
        assert_eq!(replacer.lock().unwrap().size(), 0, "Size should be 0 when replacer is empty");
        assert_eq!(replacer.lock().unwrap().total_frames(), 0, "Total frames should be 0 when replacer is empty");

        // Edge case: add an element, set it to non-evictable, then try to evict
        {
            let mut replacer = replacer.lock().unwrap();
            replacer.record_access(1, AccessType::Lookup);
            replacer.set_evictable(1, false);

            let current_size = replacer.size();
            let total_frames = replacer.total_frames();
            println!("DEBUG: Current size after adding non-evictable frame: {}", current_size);
            println!("DEBUG: Total frames after adding non-evictable frame: {}", total_frames);

            assert_eq!(current_size, 0, "Size should remain 0 after non-evictable frame is added");
            assert_eq!(total_frames, 1, "Total frames should be 1 after adding a frame");
            assert!(replacer.evict().is_none(), "Eviction of a non-evictable frame should return None");
        }

        // Edge case: set it back to evictable and evict
        {
            let mut replacer = replacer.lock().unwrap();
            replacer.set_evictable(1, true);

            let current_size = replacer.size();
            let total_frames = replacer.total_frames();
            println!("DEBUG: Current size before eviction: {}", current_size);
            println!("DEBUG: Total frames before eviction: {}", total_frames);

            assert_eq!(current_size, 1, "Size should be 1 before eviction");
            assert_eq!(total_frames, 1, "Total frames should still be 1 before eviction");
            assert_eq!(replacer.evict().unwrap(), 1, "Eviction should return frame 1");
            assert_eq!(replacer.size(), 0, "Size should be 0 after eviction");
            assert_eq!(replacer.total_frames(), 0, "Total frames should be 0 after eviction");
        }

        // Additional edge case: add multiple elements and test non-evictable scenario
        {
            let mut replacer = replacer.lock().unwrap();
            replacer.record_access(2, AccessType::Lookup);
            replacer.record_access(3, AccessType::Lookup);
            replacer.set_evictable(2, false);
            replacer.set_evictable(3, true);

            let current_size = replacer.size();
            let total_frames = replacer.total_frames();
            println!("DEBUG: Current size with one evictable and one non-evictable frame: {}", current_size);
            println!("DEBUG: Total frames with one evictable and one non-evictable frame: {}", total_frames);

            assert_eq!(current_size, 1, "Size should be 1 when one frame is evictable");
            assert_eq!(total_frames, 2, "Total frames should be 2 after adding two frames");

            // Evict the evictable frame and check
            assert_eq!(replacer.evict().unwrap(), 3, "Eviction should return frame 3");
            assert_eq!(replacer.size(), 0, "Size should be 0 after evicting the only evictable frame");
            assert_eq!(replacer.total_frames(), 1, "Total frames should be 1 with one non-evictable frame left");
        }
    }

    #[test]
    fn test_concurrent_access() {
        let mock_time_source = MockTimeSource::new();
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(10, 2, Box::new(mock_time_source))));

        let mut handles = vec![];

        // Test concurrent access by spawning multiple threads
        for i in 0..10 {
            let replacer = Arc::clone(&replacer);
            let handle = thread::spawn(move || {
                let ten_millis = time::Duration::from_millis(10);
                replacer.lock().unwrap().record_access(i, AccessType::Lookup);

                replacer.lock().unwrap().set_evictable(i, true);
            });
            handles.push(handle);
        }

        // Wait for all threads to finish
        for handle in handles {
            handle.join().unwrap();
        }

        // Evict all pages and check if they were all added
        let replacer = replacer.lock().unwrap();
        assert_eq!(10, replacer.size());

        for _ in 0..10 {
            replacer.evict().unwrap();
        }

        assert_eq!(0, replacer.size());
    }

    #[test]
    fn test_removal() {
        let mock_time_source = MockTimeSource::new();
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, Box::new(mock_time_source))));

        // Add elements to the replacer
        for i in 1..=5 {
            replacer.lock().unwrap().record_access(i, AccessType::Lookup);
            replacer.lock().unwrap().set_evictable(i, true);
        }

        // Remove some elements manually
        replacer.lock().unwrap().remove(3);
        replacer.lock().unwrap().remove(4);

        // Ensure the size is correct and evict remaining elements
        assert_eq!(3, replacer.lock().unwrap().size());

        let mut value = replacer.lock().unwrap().evict().unwrap();
        assert_ne!(value, 3);
        assert_ne!(value, 4);

        value = replacer.lock().unwrap().evict().unwrap();
        assert_ne!(value, 3);
        assert_ne!(value, 4);

        value = replacer.lock().unwrap().evict().unwrap();
        assert_ne!(value, 3);
        assert_ne!(value, 4);

        assert!(replacer.lock().unwrap().evict().is_none());
    }
}
