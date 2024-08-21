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
    pub fn new(num_frames: usize, k: usize) -> Self {
        info!("Initializing LRUKReplacer with size {} and k {}", num_frames, k);
        Self {
            frame_store: Arc::new(Mutex::new(HashMap::new())),
            replacer_size: num_frames,
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
            if k_distance < min_k_distance || (k_distance == min_k_distance && frame.access_times[0] < earliest_access_time) {
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
                debug!("Updated access times for frame {}: {:?}", frame_id, frame.access_times);
            })
            .or_insert_with(|| {
                debug!("Inserting new frame {} with initial access time {}", frame_id, now);
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
            info!("Frame {} not found in frame_store, adding with evictable status {}", frame_id, set_evictable);
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

    /// Returns the number of frames that are eligible for eviction.
    ///
    /// # Returns
    ///
    /// The number of evictable frames.
    pub fn size(&self) -> usize {
        let frame_store = self.frame_store.lock().unwrap(); // Acquire std::sync::Mutex lock
        let size = frame_store
            .iter()
            .filter(|&frame| frame.1.is_evictable)
            .count();
        debug!("Current size of evictable frames: {}", size);
        size
    }
}
