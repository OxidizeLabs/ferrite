use config::FrameId;
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy)]
pub enum AccessType {
    Unknown = 0,
    Lookup,
    Scan,
    Index
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Frame {
    /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
    access_times: Vec<u64>,
    k: usize,
    fid: FrameId,
    is_evictable: bool
}

pub struct LRUKReplacer {
    frame_store: HashMap<FrameId, Frame>,
    current_timestamp: usize,
    curr_size_: usize,
    replacer_size: usize,
    k: usize,
    latch: Mutex<usize>
}

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        let frame = Frame {
            access_times: vec![],
            k: 0,
            fid: 0,
            is_evictable: false,
        };

        Self {
            frame_store: HashMap::from([(0, frame)]),
            current_timestamp: 0,
            curr_size_: 0,
            replacer_size: num_frames,
            k,
            latch: Mutex::new(0),
        }
    }

    pub fn evict(&mut self) -> Option<FrameId> {
        /**
         * TODO(P1): Add implementation
         *
         * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
         * that are marked as 'evictable' are candidates for eviction.
         *
         * A frame with less than k historical references is given +inf as its backward k-distance.
         * If multiple frames have inf backward k-distance, then evict frame with earliest timestamp
         * based on LRU.
         *
         * Successful eviction of a frame should decrement the size of replacer and remove the frame's
         * access history.
         *
         * @param[out] frame_id id of frame that is evicted.
         * @return true if a frame is evicted successfully, false if no frames can be evicted.
         */
        let mut max_k_distance = 0;
        let mut victim_frame_id = None;

        for (id, frame) in &self.frame_store {
            if !frame.is_evictable {
                continue;
            }

            let k_distance = if frame.access_times.len() < self.k {
                u64::MAX
            } else {
                // Calculate backward k-distance
                let len = frame.access_times.len();
                frame.access_times[len - self.k] // k-th last access time
            };

            // Determine the frame to evict
            if k_distance > max_k_distance {
                max_k_distance = k_distance;
                victim_frame_id = Some(*id);
            } else if k_distance == max_k_distance {
                // If k_distance is the same, choose the frame with the earliest access time
                if let Some(current_victim_id) = victim_frame_id {
                    let current_victim_frame = &self.frame_store[&current_victim_id];
                    if frame.access_times.first() < current_victim_frame.access_times.first() {
                        victim_frame_id = Some(*id);
                    }
                }
            }
        }

        // Evict the selected frame
        if let Some(victim_id) = victim_frame_id {
            self.frame_store.remove(&victim_id);
            Some(victim_id)
        } else {
            None
        }

    }

    pub fn record_access(&mut self, frame_id: FrameId, access_type: AccessType) {
        /**
         * TODO(P1): Add implementation
         *
         * @brief Record the event that the given frame id is accessed at current timestamp.
         * Create a new entry for access history if frame id has not been seen before.
         *
         * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
         * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
         *
         * @param frame_id id of frame that received a new access.
         * @param access_type type of access that was received. This parameter is only needed for
         * leaderboard tests.
         */
        let now = Self::current_time();
        self.frame_store
            .entry(frame_id)
            .and_modify(|frame| frame.access_times.push(now))
            .or_insert(Frame {
                is_evictable: true,
                access_times: vec![now],
                k: 0,
                fid: 0,
            });
    }

    pub fn set_evictable(&mut self, frame_id: FrameId, set_evictable: bool) {
        /**
         * TODO(P1): Add implementation
         *
         * @brief Toggle whether a frame is evictable or non-evictable. This function also
         * controls replacer's size. Note that size is equal to number of evictable entries.
         *
         * If a frame was previously evictable and is to be set to non-evictable, then size should
         * decrement. If a frame was previously non-evictable and is to be set to evictable,
         * then size should increment.
         *
         * If frame id is invalid, throw an exception or abort the process.
         *
         * For other scenarios, this function should terminate without modifying anything.
         *
         * @param frame_id id of frame whose 'evictable' status will be modified
         * @param set_evictable whether the given frame is evictable or not
         */
        if let Some(frame) = self.frame_store.get_mut(&frame_id) {
            frame.is_evictable = set_evictable;
        }
    }

    pub fn remove(&self, frame_id: FrameId) {
        /**
         * TODO(P1): Add implementation
         *
         * @brief Remove an evictable frame from replacer, along with its access history.
         * This function should also decrement replacer's size if removal is successful.
         *
         * Note that this is different from evicting a frame, which always remove the frame
         * with largest backward k-distance. This function removes specified frame id,
         * no matter what its backward k-distance is.
         *
         * If Remove is called on a non-evictable frame, throw an exception or abort the
         * process.
         *
         * If specified frame is not found, directly return from this function.
         *
         * @param frame_id id of frame to be removed
         */
        unimplemented!()
    }

    pub fn size(self) -> usize {
        /**
         * TODO(P1): Add implementation
         *
         * @brief Return replacer's size, which tracks the number of evictable frames.
         *
         * @return size_t
         */
        unimplemented!()
    }

    // Helper function to get current time in milliseconds since epoch
    fn current_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}


