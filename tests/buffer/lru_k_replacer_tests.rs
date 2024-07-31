#[cfg(test)]
mod tests {
    extern crate tkdb;
    use tkdb::buffer::lru_k_replacer::{AccessType, LRUKReplacer};
    use std::sync::{Arc, Mutex};
    use std::thread::sleep;
    use std::time;

    #[test]
    fn basic_tests() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(7, 2)));

        {
            let replacer = replacer.lock().unwrap();
            let ten_millis = time::Duration::from_millis(10);

            sleep(ten_millis);

            // Scenario: add six elements to the replacer. We have [1,2,3,4,5]. Frame 6 is non-evictable.
            replacer.record_access(1, AccessType::Lookup);
            sleep(ten_millis);
            replacer.record_access(2, AccessType::Lookup);
            sleep(ten_millis);
            replacer.record_access(3, AccessType::Lookup);
            sleep(ten_millis);
            replacer.record_access(4, AccessType::Lookup);
            sleep(ten_millis);
            replacer.record_access(5, AccessType::Lookup);
            sleep(ten_millis);
            replacer.record_access(6, AccessType::Lookup);
            sleep(ten_millis);
            replacer.set_evictable(1, true);
            replacer.set_evictable(2, true);
            replacer.set_evictable(3, true);
            replacer.set_evictable(4, true);
            replacer.set_evictable(5, true);
            replacer.set_evictable(6, false);
            assert_eq!(5, replacer.size());

            // Scenario: Insert access history for frame 1. Now frame 1 has two access histories.
            // All other frames have max backward k-dist. The order of eviction is [2,3,4,5,1].
            replacer.record_access(1, AccessType::Lookup);

            // Scenario: Evict three pages from the replacer. Elements with max k-distance should be popped
            // first based on LRU.
            let mut value = replacer.evict().unwrap();
            assert_eq!(2, value);
            value = replacer.evict().unwrap();
            assert_eq!(3, value);
            value = replacer.evict().unwrap();
            assert_eq!(4, value);
            assert_eq!(2, replacer.size());

            // Scenario: Now replacer has frames [5,1].
            // Insert new frames 3, 4, and update access history for 5. We should end with [3,1,5,4]
            replacer.record_access(3, AccessType::Lookup);
            sleep(ten_millis);
            replacer.record_access(4, AccessType::Lookup);
            sleep(ten_millis);
            replacer.record_access(5, AccessType::Lookup);
            sleep(ten_millis);
            replacer.set_evictable(3, true);
            sleep(ten_millis);
            replacer.set_evictable(4, true);
            assert_eq!(4, replacer.size());

            // Scenario: continue looking for victims. We expect 3 to be evicted next.
            value = replacer.evict().unwrap();
            assert_eq!(3, value);
            assert_eq!(3, replacer.size());

            // Set 6 to be evictable. 6 Should be evicted next since it has max backward k-dist.
            replacer.set_evictable(6, true);
            assert_eq!(4, replacer.size());
            value = replacer.evict().unwrap();
            assert_eq!(6, value);
            assert_eq!(3, replacer.size());

            // Now we have [1,5,4]. Continue looking for victims.
            replacer.set_evictable(1, false);
            assert_eq!(2, replacer.size());
            value = replacer.evict().unwrap();
            assert_eq!(4, value);
            assert_eq!(1, replacer.size());

            // Update access history for 1. Now we have [5,1]. Next victim is 5.
            replacer.set_evictable(1, true);
            assert_eq!(2, replacer.size());
            value = replacer.evict().unwrap();
            assert_eq!(5, value);

            assert_eq!(1, replacer.size());
            value = replacer.evict().unwrap();
            assert_eq!(1, value);
            assert_eq!(0, replacer.size());

            // This operation should not modify size
            assert!(replacer.evict().is_none());
            assert_eq!(0, replacer.size());
        }
    }
}


