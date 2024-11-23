use crate::common::config::FrameId;
use log::{debug, error, info, warn};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// The type of access operation on a frame.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AccessType {
    /// Access type is unknown.
    Unknown = 0,
    /// Lookup access.
    Lookup,
    /// Sequential scan access.
    Scan,
    /// Index-based access.
    Index,
}

/// Represents a frame in the buffer pool.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct Frame {
    /// Stores the timestamps of accesses to this frame.
    access_times: Vec<u64>,
    /// The unique identifier of the frame.
    fid: FrameId,
    /// Whether the frame is eligible for eviction.
    is_evictable: bool,
}

/// An LRU-K page replacement policy.
///
/// This replacer evicts the frame whose K-th most recent access is furthest in the past.
pub struct LRUKReplacer {
    /// The store of frames managed by this replacer.
    frame_store: Arc<Mutex<HashMap<FrameId, Frame>>>,
    /// The maximum number of frames the replacer can manage.
    replacer_size: usize,
    /// The K value determining the K-th most recent access to consider.
    k: usize,
}

impl LRUKReplacer {
    /// Creates a new `LRUKReplacer`.
    ///
    /// # Arguments
    ///
    /// * `num_frames` - The maximum number of frames to manage.
    /// * `k` - The K value determining the K-th most recent access to consider for eviction.
    ///
    /// # Returns
    ///
    /// A new `LRUKReplacer` instance.
    pub fn new(replacer_size: usize, k: usize) -> Self {
        info!(
            "Initializing LRUKReplacer with size {} and k {}",
            replacer_size, k
        );
        Self {
            frame_store: Arc::new(Mutex::new(HashMap::with_capacity(replacer_size))),
            replacer_size,
            k,
        }
    }

    /// Retrieves the current time in microseconds.
    ///
    /// # Returns
    ///
    /// The current time in microseconds since the Unix epoch.
    fn current_time_in_micros() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    /// Evicts a frame based on the LRU-K policy.
    ///
    /// # Returns
    ///
    /// The `FrameId` of the evicted frame, or `None` if no frame could be evicted.
    pub fn evict(&self) -> Option<FrameId> {
        let mut frame_store = self.frame_store.lock().unwrap();
        let mut victim_frame_id = None;
        let mut min_k_distance = u64::MAX;
        let mut earliest_access_time = u64::MAX;

        for (&id, frame) in frame_store.iter() {
            if !frame.is_evictable {
                continue;
            }

            // Calculate k_distance for the frame
            let k_distance = if frame.access_times.len() < self.k {
                // Favor eviction of frames with fewer than `k` accesses by assigning a high k_distance
                u64::MIN
            } else {
                // Use the k-th most recent access time if there are enough accesses
                let len = frame.access_times.len();
                frame.access_times[len - self.k]
            };

            // Eviction decision based on k_distance and earliest access time as a tie-breaker
            if k_distance < min_k_distance
                || (k_distance == min_k_distance && frame.access_times[0] < earliest_access_time)
            {
                min_k_distance = k_distance;
                earliest_access_time = frame.access_times[0];
                victim_frame_id = Some(id);
            }
        }

        if let Some(victim_id) = victim_frame_id {
            if frame_store.remove(&victim_id).is_some() {
                info!("Evicting frame {}", victim_id);
                return Some(victim_id);
            }
        }

        warn!("Failed to evict a frame.");
        None
    }

    /// Records an access to a frame.
    ///
    /// # Arguments
    ///
    /// * `frame_id` - The ID of the frame being accessed.
    /// * `access_type` - The type of access being performed.
    pub fn record_access(&self, frame_id: FrameId, _access_type: AccessType) {
        let now = Self::current_time_in_micros();
        let mut frame_store = self.frame_store.lock().unwrap(); // Acquire std::sync::Mutex lock

        frame_store
            .entry(frame_id)
            .and_modify(|frame| {
                frame.access_times.push(now);
                if frame.access_times.len() > self.k {
                    frame.access_times.remove(0); // Trim the vector to only keep the last `k` accesses
                }
                debug!(
                    "Updated access times for frame {}: {:?}",
                    frame_id, frame.access_times
                );
            })
            .or_insert_with(|| {
                debug!(
                    "Inserting new frame {} with initial access time {}",
                    frame_id, now
                );
                Frame {
                    is_evictable: true,
                    access_times: vec![now],
                    fid: frame_id,
                }
            });

        info!("Recorded access for frame {} at {}", frame_id, now);
    }

    /// Sets whether a frame is eligible for eviction.
    ///
    /// # Arguments
    ///
    /// * `frame_id` - The ID of the frame to update.
    /// * `set_evictable` - Whether the frame should be evictable.
    pub fn set_evictable(&self, frame_id: FrameId, set_evictable: bool) {
        let mut frame_store = self.frame_store.lock().unwrap(); // Acquire std::sync::Mutex lock
        if let Some(frame) = frame_store.get_mut(&frame_id) {
            frame.is_evictable = set_evictable;
            debug!("Set frame {} evictable: {}", frame_id, set_evictable);
        } else {
            info!(
                "Frame {} not found in frame_store, adding with evictable status {}",
                frame_id, set_evictable
            );
            frame_store.insert(
                frame_id,
                Frame {
                    is_evictable: set_evictable,
                    access_times: vec![],
                    fid: frame_id,
                },
            );
        }
    }

    /// Removes a frame from the replacer.
    ///
    /// # Arguments
    ///
    /// * `frame_id` - The ID of the frame to remove.
    pub fn remove(&self, frame_id: FrameId) {
        let mut frame_store = self.frame_store.lock().unwrap(); // Acquire std::sync::Mutex lock
        if let Some(frame) = frame_store.get(&frame_id) {
            if frame.is_evictable {
                info!("Removing frame {}", frame_id);
                frame_store.remove(&frame_id);
            } else {
                error!("Attempt to remove a non-evictable frame {}", frame_id);
            }
        } else {
            info!("Attempt to remove a non-existing frame {}", frame_id);
        }
    }

    /// Returns the total number of frames managed by the replacer.
    ///
    /// # Returns
    ///
    /// The total number of frames.
    pub fn total_frames(&self) -> usize {
        let frame_store = self.frame_store.lock().unwrap(); // Acquire std::sync::Mutex lock
        let total = frame_store.len();
        debug!("Current total frames: {}", total);
        total
    }

    /// Returns the capacity of the replacer
    ///
    /// # Returns
    ///
    /// The number of frames the replacer can hold.
    pub fn get_replacer_size(&self) -> usize {
        self.replacer_size
    }

    /// Returns the number of frames that are eligible for eviction.
    ///
    /// # Returns
    ///
    /// The number of evictable frames.
    pub fn total_evictable_frames(&self) -> usize {
        let frame_store = self.frame_store.lock().unwrap(); // Acquire std::sync::Mutex lock
        let size = frame_store
            .iter()
            .filter(|&frame| frame.1.is_evictable)
            .count();
        debug!("Current size of evictable frames: {}", size);
        size
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use std::thread::sleep;
    use std::time::Duration;

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
        assert_eq!(
            evicted_frame,
            Some(1),
            "Expected frame 1 to be evicted first"
        );

        let evicted_frame = replacer.evict();
        assert_eq!(
            evicted_frame,
            Some(2),
            "Expected frame 2 to be evicted next"
        );
    }

    #[test]
    fn evict_non_evictable_frame() {
        let replacer = LRUKReplacer::new(5, 2);

        replacer.record_access(1, AccessType::Lookup);
        replacer.set_evictable(1, false); // Make frame 1 non-evictable

        let evicted_frame = replacer.evict();
        assert_eq!(
            evicted_frame, None,
            "Expected no frame to be evicted since frame 1 is not evictable"
        );
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
        assert_eq!(
            evicted_frame,
            Some(2),
            "Expected frame 2 to be evicted based on LRU-K policy"
        );
    }

    #[test]
    fn evict_when_all_frames_are_non_evictable() {
        let replacer = LRUKReplacer::new(5, 2);

        replacer.record_access(1, AccessType::Lookup);
        replacer.record_access(2, AccessType::Lookup);

        replacer.set_evictable(1, false);
        replacer.set_evictable(2, false);

        let evicted_frame = replacer.evict();
        assert_eq!(
            evicted_frame, None,
            "Expected no frame to be evicted since all frames are non-evictable"
        );
    }

    #[test]
    fn evict_respects_eviction_order() {
        let replacer = LRUKReplacer::new(5, 2);

        replacer.record_access(1, AccessType::Lookup);
        replacer.record_access(2, AccessType::Lookup);
        replacer.record_access(3, AccessType::Lookup);

        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        replacer.set_evictable(3, true);

        let evicted_frame = replacer.evict();
        assert_eq!(
            evicted_frame,
            Some(1),
            "Expected frame 1 to be evicted first"
        );

        let evicted_frame = replacer.evict();
        assert_eq!(
            evicted_frame,
            Some(2),
            "Expected frame 2 to be evicted next"
        );

        let evicted_frame = replacer.evict();
        assert_eq!(
            evicted_frame,
            Some(3),
            "Expected frame 3 to be evicted last"
        );
    }

    #[test]
    fn basic_lru_tests() {
        let replacer = LRUKReplacer::new(5, 2);

        replacer.record_access(1, AccessType::Lookup);
        replacer.record_access(2, AccessType::Lookup);
        replacer.record_access(3, AccessType::Lookup);
        replacer.record_access(4, AccessType::Lookup);
        replacer.record_access(5, AccessType::Lookup);

        replacer.set_evictable(1, true);
        replacer.set_evictable(2, true);
        replacer.set_evictable(3, true);
        replacer.set_evictable(4, true);
        replacer.set_evictable(5, true);

        assert_eq!(
            replacer.total_evictable_frames(),
            5,
            "Size should be 5 when 5 frames are evictable"
        );
        assert_eq!(
            replacer.total_frames(),
            5,
            "Total frames should be 5 after adding all frames"
        );

        // Evict 3 pages
        let mut evicted = replacer.evict();
        assert_eq!(evicted, Some(1), "First evicted frame should be 1");

        evicted = replacer.evict();
        assert_eq!(evicted, Some(2), "Second evicted frame should be 2");

        evicted = replacer.evict();
        assert_eq!(evicted, Some(3), "Third evicted frame should be 3");

        assert_eq!(
            replacer.total_evictable_frames(),
            2,
            "Size should be 2 after three evictions"
        );
    }

    #[test]
    fn frame_removal() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));
        replacer
            .lock()
            .unwrap()
            .record_access(1, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(2, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(3, AccessType::Lookup);
        replacer.lock().unwrap().set_evictable(1, true);
        replacer.lock().unwrap().set_evictable(2, true);
        replacer.lock().unwrap().set_evictable(3, true);

        // Remove frame 2
        replacer.lock().unwrap().remove(2);
        assert_eq!(
            replacer.lock().unwrap().total_evictable_frames(),
            2,
            "Size should be 2 after removing frame 2"
        );

        // Ensure evicting does not remove the already-removed frame
        let evicted = replacer.lock().unwrap().evict();
        assert_ne!(
            evicted,
            Some(2),
            "Frame 2 should not be evicted as it was removed"
        );
    }
}

#[cfg(test)]
mod basic_behaviour {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn basic_eviction_order() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));

        replacer
            .lock()
            .unwrap()
            .record_access(1, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(2, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(3, AccessType::Lookup);

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

        assert_eq!(
            replacer.lock().unwrap().total_evictable_frames(),
            0,
            "Size should be 0 after all evictions"
        );
    }

    #[test]
    fn mixed_access_pattern() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));

        replacer
            .lock()
            .unwrap()
            .record_access(1, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(2, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(3, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(4, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(5, AccessType::Lookup);

        replacer.lock().unwrap().set_evictable(1, true);
        replacer.lock().unwrap().set_evictable(2, true);
        replacer.lock().unwrap().set_evictable(3, true);
        replacer.lock().unwrap().set_evictable(4, true);
        replacer.lock().unwrap().set_evictable(5, true);

        // Access frames multiple times in mixed order
        replacer
            .lock()
            .unwrap()
            .record_access(5, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(3, AccessType::Lookup);
        replacer
            .lock()
            .unwrap()
            .record_access(2, AccessType::Lookup);

        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(
            evicted, 1,
            "First evicted frame should be 1 (least recently accessed)"
        );

        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(
            evicted, 4,
            "Second evicted frame should be 4 (due to its K-th access pattern)"
        );

        let evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(
            evicted, 2,
            "Third evicted frame should be 2 (next in line by access pattern)"
        );

        assert_eq!(
            replacer.lock().unwrap().total_evictable_frames(),
            2,
            "Size should be 2 after three evictions"
        );
    }
}

#[cfg(test)]
mod concurrency {
    use super::*;
    use std::thread;

    #[test]
    fn concurrent_access() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(10, 2)));

        let mut handles = vec![];

        for i in 1..=10 {
            let replacer = Arc::clone(&replacer);
            let handle = thread::spawn(move || {
                let replacer_lock = replacer.lock().unwrap();
                replacer_lock.record_access(i, AccessType::Lookup);
                replacer_lock.set_evictable(i, true);
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // Verify that all frames are added
        assert_eq!(
            replacer.lock().unwrap().total_evictable_frames(),
            10,
            "All frames should be present after concurrent access"
        );

        for _ in 0..10 {
            replacer.lock().unwrap().evict().unwrap();
        }

        assert_eq!(
            replacer.lock().unwrap().total_evictable_frames(),
            0,
            "Size should be 0 after all evictions"
        );
    }
}

#[cfg(test)]
mod edge_cases {
    use super::*;

    #[test]
    fn evict_from_empty() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));

        // Edge case: try to evict from an empty replacer
        assert!(
            replacer.lock().unwrap().evict().is_none(),
            "Eviction from an empty replacer should return None"
        );
        assert_eq!(
            replacer.lock().unwrap().total_evictable_frames(),
            0,
            "Size should be 0 when replacer is empty"
        );
        assert_eq!(
            replacer.lock().unwrap().total_frames(),
            0,
            "Total frames should be 0 when replacer is empty"
        );

        // Edge case: add an element, set it to non-evictable, then try to evict
        replacer
            .lock()
            .unwrap()
            .record_access(1, AccessType::Lookup);
        replacer.lock().unwrap().set_evictable(1, false);

        assert_eq!(
            replacer.lock().unwrap().total_evictable_frames(),
            0,
            "Size should be 0 with non-evictable frame"
        );
        assert_eq!(
            replacer.lock().unwrap().total_frames(),
            1,
            "Total frames should be 1 with non-evictable frame"
        );
        assert!(
            replacer.lock().unwrap().evict().is_none(),
            "Eviction of a non-evictable frame should return None"
        );

        // Edge case: set it back to evictable and evict
        replacer.lock().unwrap().set_evictable(1, true);
        assert_eq!(
            replacer.lock().unwrap().evict().unwrap(),
            1,
            "Eviction should return frame 1"
        );
    }

    #[test]
    fn multiple_eviction_candidates() {
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(5, 2)));

        // Add frames and set them all to evictable
        for i in 1..=5 {
            replacer
                .lock()
                .unwrap()
                .record_access(i, AccessType::Lookup);
            replacer.lock().unwrap().set_evictable(i, true);
        }

        // Evict frames and verify eviction order (LRU-K order)
        let mut evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 1, "First evicted frame should be 1");

        evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 2, "Second evicted frame should be 2");

        evicted = replacer.lock().unwrap().evict().unwrap();
        assert_eq!(evicted, 3, "Third evicted frame should be 3");

        assert_eq!(
            replacer.lock().unwrap().total_evictable_frames(),
            2,
            "Size should be 2 after three evictions"
        );
    }
}
