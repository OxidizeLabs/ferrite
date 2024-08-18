use log::{debug, error, info};
use spin::Mutex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::common::config::FrameId;
use crate::common::time::TimeSource;

#[derive(Clone, Copy)]
pub enum AccessType {
    Unknown = 0,
    Lookup,
    Scan,
    Index,
}

#[derive(Hash, Eq, PartialEq, Debug)]
pub struct Frame {
    access_times: Vec<u64>,
    k: usize,
    fid: FrameId,
    is_evictable: bool,
}

pub struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }
}

pub struct LRUKReplacer {
    frame_store: Arc<Mutex<HashMap<FrameId, Frame>>>,
    time_source: Arc<dyn TimeSource>,
    replacer_size: usize,
    k: usize,
}

impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize, time_source: Arc<dyn TimeSource>) -> Self {
        info!("Initializing LRUKReplacer with size {} and k {}", num_frames, k);
        Self {
            frame_store: Arc::new(Mutex::new(HashMap::new())),
            time_source,
            replacer_size: num_frames,
            k,
        }
    }

    pub fn evict(&self) -> Option<FrameId> {
        let mut frame_store = self.frame_store.lock(); // Acquire spin lock
        let mut max_k_distance = 0;
        let mut victim_frame_id = None;

        debug!("Starting eviction process");
        for (id, frame) in frame_store.iter() {
            if !frame.is_evictable {
                debug!("Frame {} is not evictable, skipping", id);
                continue;
            }

            let k_distance = if frame.access_times.len() < self.k {
                u64::MAX
            } else {
                let len = frame.access_times.len();
                frame.access_times[len - self.k]
            };

            if k_distance > max_k_distance {
                max_k_distance = k_distance;
                victim_frame_id = Some(*id);
                debug!("Frame {} is a new eviction candidate with k-distance {}", id, k_distance);
            } else if k_distance == max_k_distance {
                if let Some(current_victim_id) = victim_frame_id {
                    let current_victim_frame = &frame_store[&current_victim_id];
                    if frame.access_times.first() < current_victim_frame.access_times.first() {
                        victim_frame_id = Some(*id);
                        debug!("Frame {} is a better candidate than {}", id, current_victim_id);
                    }
                }
            }
        }

        if let Some(victim_id) = victim_frame_id {
            info!("Evicting frame {}", victim_id);
            frame_store.remove(&victim_id);
            Some(victim_id)
        } else {
            error!("No frames available for eviction");
            None
        }
    }

    pub fn record_access(&self, frame_id: FrameId, _access_type: AccessType) {
        let now = self.time_source.now();
        let mut frame_store = self.frame_store.lock(); // Acquire spin lock

        frame_store
            .entry(frame_id)
            .and_modify(|frame| {
                frame.access_times.push(now);
                debug!("Updated access times for frame {}: {:?}", frame_id, frame.access_times);
            })
            .or_insert_with(|| {
                debug!("Inserting new frame {} with initial access time {}", frame_id, now);
                Frame {
                    is_evictable: true,
                    access_times: vec![now],
                    k: self.k,
                    fid: frame_id,
                }
            });

        info!("Recorded access for frame {} at {}", frame_id, now);
    }

    pub fn set_evictable(&self, frame_id: FrameId, set_evictable: bool) {
        let mut frame_store = self.frame_store.lock(); // Acquire spin lock
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
                    k: self.k,
                    fid: frame_id,
                },
            );
        }
    }

    pub fn remove(&self, frame_id: FrameId) {
        let mut frame_store = self.frame_store.lock(); // Acquire spin lock
        if let Some(frame) = frame_store.get(&frame_id) {
            if frame.is_evictable {
                info!("Removing frame {}", frame_id);
                frame_store.remove(&frame_id);
            } else {
                error!("Attempt to remove a non-evictable frame {}", frame_id);
                panic!("Attempt to remove a non-evictable frame {}", frame_id);
            }
        } else {
            info!("Attempt to remove a non-existing frame {}", frame_id);
        }
    }

    pub fn total_frames(&self) -> usize {
        let frame_store = self.frame_store.lock(); // Acquire spin lock
        let total = frame_store.len(); // Count all frames
        debug!("Current total frames: {}", total);
        total
    }

    pub fn size(&self) -> usize {
        let frame_store = self.frame_store.lock(); // Acquire spin lock
        let size = frame_store
            .iter()
            .filter(|&frame| frame.1.is_evictable)
            .count();
        debug!("Current size of evictable frames: {}", size);
        size
    }
}
