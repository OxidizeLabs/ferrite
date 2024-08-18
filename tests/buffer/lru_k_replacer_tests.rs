extern crate tkdb;
use spin::Mutex;
use std::sync::Arc;
use std::thread;
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
        let mut time = self.time.lock();
        *time += self.increment;
    }

    pub fn get_time(&self) -> u64 {
        let time = self.time.lock();
        *time
    }

    pub fn set_increment(&self, increment: u64) {
        let mut inc = self.time.lock();
        *inc = increment;
    }
}

impl TimeSource for MockTimeSource {
    fn now(&self) -> u64 {
        self.get_time()
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn basic_lru_tests() {
        let mock_time = Arc::new(MockTimeSource::new());
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, mock_time.clone())));

        {
            let mut replacer = replacer.lock();

            replacer.record_access(1, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(2, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(3, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(4, AccessType::Lookup);
            mock_time.advance_time();
            replacer.record_access(5, AccessType::Lookup);

            replacer.set_evictable(1, true);
            replacer.set_evictable(2, true);
            replacer.set_evictable(3, true);
            replacer.set_evictable(4, true);
            replacer.set_evictable(5, true);

            assert_eq!(replacer.size(), 5, "Size should be 5 when 5 frames are evictable");
            assert_eq!(replacer.total_frames(), 5, "Total frames should be 5 after adding all frames");

            // Access frame 1 twice
            replacer.record_access(1, AccessType::Lookup);

            // Evict 3 pages
            let mut evicted = replacer.evict().unwrap();
            assert_eq!(evicted, 2, "First evicted frame should be 2");

            evicted = replacer.evict().unwrap();
            assert_eq!(evicted, 3, "Second evicted frame should be 3");

            evicted = replacer.evict().unwrap();
            assert_eq!(evicted, 4, "Third evicted frame should be 4");

            assert_eq!(replacer.size(), 2, "Size should be 2 after three evictions");
        }
    }

    #[test]
    fn frame_removal() {
        let mock_time = Arc::new(MockTimeSource::new());
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, mock_time.clone())));
        mock_time.advance_time();
        replacer.lock().record_access(1, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(2, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(3, AccessType::Lookup);
        replacer.lock().set_evictable(1, true);
        replacer.lock().set_evictable(2, true);
        replacer.lock().set_evictable(3, true);

        // Remove frame 2
        replacer.lock().remove(2);
        assert_eq!(replacer.lock().size(), 2, "Size should be 2 after removing frame 2");

        // Ensure evicting does not remove the already-removed frame
        let evicted = replacer.lock().evict().unwrap();
        assert_ne!(evicted, 2, "Frame 2 should not be evicted as it was removed");
    }
}

#[cfg(test)]
mod basic_behaviour {
    use super::*;

    #[test]
    fn basic_eviction_order() {
        let mock_time = Arc::new(MockTimeSource::new());
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, mock_time.clone())));

        replacer.lock().record_access(1, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(2, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(3, AccessType::Lookup);

        replacer.lock().set_evictable(1, true);
        replacer.lock().set_evictable(2, true);
        replacer.lock().set_evictable(3, true);

        // Verify the eviction order
        let evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 1, "First evicted frame should be 1");

        let evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 2, "Second evicted frame should be 2");

        let evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 3, "Third evicted frame should be 3");

        assert_eq!(replacer.lock().size(), 0, "Size should be 0 after all evictions");
    }

    #[test]
    fn mixed_access_pattern() {
        let mock_time = Arc::new(MockTimeSource::new());
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, mock_time.clone())));
        mock_time.advance_time();
        replacer.lock().record_access(1, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(2, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(3, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(4, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(5, AccessType::Lookup);
        mock_time.advance_time();

        replacer.lock().set_evictable(1, true);
        replacer.lock().set_evictable(2, true);
        replacer.lock().set_evictable(3, true);
        replacer.lock().set_evictable(4, true);
        replacer.lock().set_evictable(5, true);

        // Access frames multiple times in mixed order
        replacer.lock().record_access(5, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(3, AccessType::Lookup);
        mock_time.advance_time();
        replacer.lock().record_access(2, AccessType::Lookup);
        mock_time.advance_time();

        let evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 1, "First evicted frame should be 1 (least recently used)");

        let evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 4, "Second evicted frame should be 4");

        let evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 5, "Third evicted frame should be 5");

        assert_eq!(replacer.lock().size(), 2, "Size should be 5 after three evictions");
    }
}

#[cfg(test)]
mod concurrency {
    use super::*;

    #[test]
    fn concurrent_access() {
        let mock_time = Arc::new(MockTimeSource::new());
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(10, 2, mock_time.clone())));

        let mut handles = vec![];

        for i in 0..10 {
            let replacer = Arc::clone(&replacer);
            let handle = thread::spawn(move || {
                replacer.lock().record_access(i, AccessType::Lookup);
                replacer.lock().set_evictable(i, true);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify that all frames are added
        assert_eq!(replacer.lock().size(), 10, "All frames should be present after concurrent access");

        for _ in 0..10 {
            replacer.lock().evict().unwrap();
        }

        assert_eq!(replacer.lock().size(), 0, "Size should be 0 after all evictions");
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    fn evict_from_empty() {
        let mock_time = Arc::new(MockTimeSource::new());
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, mock_time.clone())));

        // Edge case: try to evict from an empty replacer
        assert!(replacer.lock().evict().is_none(), "Eviction from an empty replacer should return None");
        assert_eq!(replacer.lock().size(), 0, "Size should be 0 when replacer is empty");
        assert_eq!(replacer.lock().total_frames(), 0, "Total frames should be 0 when replacer is empty");

        // Edge case: add an element, set it to non-evictable, then try to evict
        replacer.lock().record_access(1, AccessType::Lookup);
        replacer.lock().set_evictable(1, false);

        assert_eq!(replacer.lock().size(), 0, "Size should be 0 with non-evictable frame");
        assert_eq!(replacer.lock().total_frames(), 1, "Total frames should be 1 with non-evictable frame");
        assert!(replacer.lock().evict().is_none(), "Eviction of a non-evictable frame should return None");

        // Edge case: set it back to evictable and evict
        replacer.lock().set_evictable(1, true);
        assert_eq!(replacer.lock().evict().unwrap(), 1, "Eviction should return frame 1");
    }

    #[test]
    fn multiple_eviction_candidates() {
        let mock_time = Arc::new(MockTimeSource::new());
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2, mock_time.clone())));

        // Add frames and set them all to evictable
        for i in 1..=5 {
            mock_time.advance_time();
            replacer.lock().record_access(i, AccessType::Lookup);
            replacer.lock().set_evictable(i, true);
        }

        // Evict frames and verify eviction order (LRU order)
        let mut evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 1, "First evicted frame should be 1");

        evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 2, "Second evicted frame should be 2");

        evicted = replacer.lock().evict().unwrap();
        assert_eq!(evicted, 3, "Third evicted frame should be 3");

        assert_eq!(replacer.lock().size(), 2, "Size should be 2 after three evictions");
    }
}
