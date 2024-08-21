extern crate tkdb;
use std::sync::{Arc, Mutex};
use tkdb::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn evict_single_frame() {
        let replacer = LRUKReplacer::new(5, 2);

        replacer.record_access(1, AccessType::Lookup);
        replacer.set_evictable(1, true);

        let evicted_frame = replacer.evict();
        assert_eq!(evicted_frame, Some(1), "Expected frame 1 to be evicted");
    }

    #[test]
    fn evict_multiple_frames() {
        let replacer = LRUKReplacer::new(5, 2);

        replacer.record_access(1, AccessType::Lookup);
        replacer.record_access(2, AccessType::Lookup);
        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);

        let evicted_frame = replacer.evict();
        assert_eq!(evicted_frame, Some(1), "Expected frame 1 to be evicted first");

        let evicted_frame = replacer.evict();
        assert_eq!(evicted_frame, Some(2), "Expected frame 2 to be evicted next");
    }

    #[test]
    fn evict_non_evictable_frame() {
        let replacer = LRUKReplacer::new(5, 2);

        replacer.record_access(1, AccessType::Lookup);
        replacer.set_evictable(1, false); // Make frame 1 non-evictable

        let evicted_frame = replacer.evict();
        assert_eq!(evicted_frame, None, "Expected no frame to be evicted since frame 1 is not evictable");
    }

    #[test]
    fn evict_based_on_k_distance() {
        let replacer = LRUKReplacer::new(5, 2);

        // Frame 1: Access twice, should be kept
        replacer.record_access(1, AccessType::Lookup);
        sleep(Duration::from_millis(1)); // Simulate time passing
        replacer.record_access(1, AccessType::Lookup); // Second access for frame 1

        // Frame 2: Access once, should be evicted
        sleep(Duration::from_millis(1)); // Simulate time passing
        replacer.record_access(2, AccessType::Lookup);

        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);

        // Frame 2 should be evicted because it only has one access, while frame 1 has more recent accesses.
        let evicted_frame = replacer.evict();
        assert_eq!(evicted_frame, Some(2), "Expected frame 2 to be evicted based on LRU-K policy");
    }


    // #[test]
    // fn evict_when_all_frames_are_non_evictable() {
    //     let replacer = LRUKReplacer::new(5, 2);
    //
    //     replacer.record_access(1, AccessType::Lookup);
    //     replacer.record_access(2, AccessType::Lookup);
    //
    //     replacer.set_evictable(1, false);
    //     replacer.set_evictable(2, false);
    //
    //     let evicted_frame = replacer.evict();
    //     assert_eq!(evicted_frame, None, "Expected no frame to be evicted since all frames are non-evictable");
    // }
    //
    // #[test]
    // fn evict_respects_eviction_order() {
    //     let replacer = LRUKReplacer::new(5, 2);
    //
    //     replacer.record_access(1, AccessType::Lookup);
    //     replacer.record_access(2, AccessType::Lookup);
    //     replacer.record_access(3, AccessType::Lookup);
    //
    //     replacer.set_evictable(1, true);
    //     replacer.set_evictable(2, true);
    //     replacer.set_evictable(3, true);
    //
    //     let evicted_frame = replacer.evict();
    //     assert_eq!(evicted_frame, Some(1), "Expected frame 1 to be evicted first");
    //
    //     let evicted_frame = replacer.evict();
    //     assert_eq!(evicted_frame, Some(2), "Expected frame 2 to be evicted next");
    //
    //     let evicted_frame = replacer.evict();
    //     assert_eq!(evicted_frame, Some(3), "Expected frame 3 to be evicted last");
    // }
    //
    // #[test]
    // fn basic_lru_tests() {
    //     let replacer = LRUKReplacer::new(5, 2);
    //
    //     replacer.record_access(1, AccessType::Lookup);
    //     replacer.record_access(2, AccessType::Lookup);
    //     replacer.record_access(3, AccessType::Lookup);
    //     replacer.record_access(4, AccessType::Lookup);
    //     replacer.record_access(5, AccessType::Lookup);
    //
    //     replacer.set_evictable(1, true);
    //     replacer.set_evictable(2, true);
    //     replacer.set_evictable(3, true);
    //     replacer.set_evictable(4, true);
    //     replacer.set_evictable(5, true);
    //
    //     assert_eq!(replacer.size(), 5, "Size should be 5 when 5 frames are evictable");
    //     assert_eq!(replacer.total_frames(), 5, "Total frames should be 5 after adding all frames");
    //
    //     // Evict 3 pages
    //     let mut evicted = replacer.evict();
    //     assert_eq!(evicted, Some(1), "First evicted frame should be 1");
    //
    //     evicted = replacer.evict();
    //     assert_eq!(evicted, Some(2), "Second evicted frame should be 2");
    //
    //     evicted = replacer.evict();
    //     assert_eq!(evicted, Some(3), "Third evicted frame should be 3");
    //
    //     assert_eq!(replacer.size(), 2, "Size should be 2 after three evictions");
    // }
    //
    // #[test]
    // fn frame_removal() {
    //     let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));
    //     replacer.lock().unwrap().record_access(1, AccessType::Lookup);
    //     replacer.lock().unwrap().record_access(2, AccessType::Lookup);
    //     replacer.lock().unwrap().record_access(3, AccessType::Lookup);
    //     replacer.lock().unwrap().set_evictable(1, true);
    //     replacer.lock().unwrap().set_evictable(2, true);
    //     replacer.lock().unwrap().set_evictable(3, true);
    //
    //     // Remove frame 2
    //     replacer.lock().unwrap().remove(2);
    //     assert_eq!(replacer.lock().unwrap().size(), 2, "Size should be 2 after removing frame 2");
    //
    //     // Ensure evicting does not remove the already-removed frame
    //     let evicted = replacer.lock().unwrap().evict();
    //     assert_ne!(evicted, Some(2), "Frame 2 should not be evicted as it was removed");
    // }
}

#[cfg(test)]
mod basic_behaviour {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn basic_eviction_order() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));

        replacer.lock().unwrap().record_access(1, AccessType::Lookup);
        replacer.lock().unwrap().record_access(2, AccessType::Lookup);
        replacer.lock().unwrap().record_access(3, AccessType::Lookup);

        replacer.lock().unwrap().set_evictable(1, true);
        replacer.lock().unwrap().set_evictable(2, true);
        replacer.lock().unwrap().set_evictable(3, true);

        // Verify the eviction order
        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 1, "First evicted frame should be 1");

        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 2, "Second evicted frame should be 2");

        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 3, "Third evicted frame should be 3");

        assert_eq!(replacer.lock().unwrap().size(), 0, "Size should be 0 after all evictions");
    }

    #[test]
    fn mixed_access_pattern() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));

        replacer.lock().unwrap().record_access(1, AccessType::Lookup);
        replacer.lock().unwrap().record_access(2, AccessType::Lookup);
        replacer.lock().unwrap().record_access(3, AccessType::Lookup);
        replacer.lock().unwrap().record_access(4, AccessType::Lookup);
        replacer.lock().unwrap().record_access(5, AccessType::Lookup);

        replacer.lock().unwrap().set_evictable(1, true);
        replacer.lock().unwrap().set_evictable(2, true);
        replacer.lock().unwrap().set_evictable(3, true);
        replacer.lock().unwrap().set_evictable(4, true);
        replacer.lock().unwrap().set_evictable(5, true);

        // Access frames multiple times in mixed order
        replacer.lock().unwrap().record_access(5, AccessType::Lookup);
        replacer.lock().unwrap().record_access(3, AccessType::Lookup);
        replacer.lock().unwrap().record_access(2, AccessType::Lookup);

        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 1, "First evicted frame should be 1 (least recently accessed)");

        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 4, "Second evicted frame should be 4 (due to its K-th access pattern)");

        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 2, "Third evicted frame should be 2 (next in line by access pattern)");

        assert_eq!(replacer.lock().unwrap().size(), 2, "Size should be 2 after three evictions");
    }
}

#[cfg(test)]
mod concurrency {
    use super::*;

    #[test]
    fn concurrent_access() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(10, 2)));

        let mut handles = vec![];

        for i in 1..=10 {
            let replacer = Arc::clone(&replacer);
            let handle = thread::spawn(move || {
                let mut replacer_lock = replacer.lock().unwrap();
                replacer_lock.record_access(i, AccessType::Lookup);
                replacer_lock.set_evictable(i, true);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify that all frames are added
        assert_eq!(replacer.lock().unwrap().size(), 10, "All frames should be present after concurrent access");

        for _ in 0..10 {
            replacer.lock().unwrap().evict().unwrap();
        }

        assert_eq!(replacer.lock().unwrap().size(), 0, "Size should be 0 after all evictions");
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    fn evict_from_empty() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));

        // Edge case: try to evict from an empty replacer
        assert!(replacer.lock().unwrap().evict().is_none(), "Eviction from an empty replacer should return None");
        assert_eq!(replacer.lock().unwrap().size(), 0, "Size should be 0 when replacer is empty");
        assert_eq!(replacer.lock().unwrap().total_frames(), 0, "Total frames should be 0 when replacer is empty");

        // Edge case: add an element, set it to non-evictable, then try to evict
        replacer.lock().unwrap().record_access(1, AccessType::Lookup);
        replacer.lock().unwrap().set_evictable(1, false);

        assert_eq!(replacer.lock().unwrap().size(), 0, "Size should be 0 with non-evictable frame");
        assert_eq!(replacer.lock().unwrap().total_frames(), 1, "Total frames should be 1 with non-evictable frame");
        assert!(replacer.lock().unwrap().evict().is_none(), "Eviction of a non-evictable frame should return None");

        // Edge case: set it back to evictable and evict
        replacer.lock().unwrap().set_evictable(1, true);
        assert_eq!(replacer.lock().unwrap().evict().unwrap(), 1, "Eviction should return frame 1");
    }

    #[test]
    fn multiple_eviction_candidates() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));

        // Add frames and set them all to evictable
        for i in 1..=5 {
            replacer.lock().unwrap().record_access(i, AccessType::Lookup);
            replacer.lock().unwrap().set_evictable(i, true);
        }

        // Evict frames and verify eviction order (LRU-K order)
        let mut evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 1, "First evicted frame should be 1");

        evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 2, "Second evicted frame should be 2");

        evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 3, "Third evicted frame should be 3");

        assert_eq!(replacer.lock().unwrap().size(), 2, "Size should be 2 after three evictions");
    }
}
