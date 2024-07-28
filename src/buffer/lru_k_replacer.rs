use crate::common::config::FrameId;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

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

pub struct LRUKReplacer {
    frame_store: Arc<Mutex<HashMap<FrameId, Frame>>>,
    current_timestamp: Mutex<usize>,
    curr_size_: Mutex<usize>,
    replacer_size: usize,
    k: usize,
}

impl LRUKReplacer {
    pub fn new(num_frames: usize, k: usize) -> Self {
        Self {
            frame_store: Arc::new(Mutex::new(HashMap::new())),
            current_timestamp: Mutex::new(0),
            curr_size_: Mutex::new(0),
            replacer_size: num_frames,
            k,
        }
    }

    pub fn evict(&self) -> Option<FrameId> {
        let mut frame_store = self.frame_store.lock().unwrap();
        let mut max_k_distance = 0;
        let mut victim_frame_id = None;

        for (id, frame) in frame_store.iter() {
            if !frame.is_evictable {
                println!("Frame {} is not evictable", id); // Debugging statement
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
            } else if k_distance == max_k_distance {
                if let Some(current_victim_id) = victim_frame_id {
                    let current_victim_frame = &frame_store[&current_victim_id];
                    if frame.access_times.first() < current_victim_frame.access_times.first() {
                        victim_frame_id = Some(*id);
                    }
                }
            }
        }

        if let Some(victim_id) = victim_frame_id {
            println!("Evicting frame {}", victim_id); // Debugging statement
            frame_store.remove(&victim_id);
            Some(victim_id)
        } else {
            println!("No frames available for eviction"); // Debugging statement
            None
        }
    }

    pub fn record_access(&self, frame_id: FrameId, _access_type: AccessType) {
        let now = Self::current_time();
        let mut frame_store = self.frame_store.lock().unwrap();

        frame_store
            .entry(frame_id)
            .and_modify(|frame| frame.access_times.push(now))
            .or_insert(Frame {
                is_evictable: true,
                access_times: vec![now],
                k: self.k,
                fid: frame_id,
            });

        println!("Recorded access for frame {} at {}", frame_id, now); // Debugging statement
    }

    pub fn set_evictable(&self, frame_id: FrameId, set_evictable: bool) {
        let mut frame_store = self.frame_store.lock().unwrap();
        if let Some(frame) = frame_store.get_mut(&frame_id) {
            frame.is_evictable = set_evictable;
            println!("Set frame {} evictable: {}", frame_id, set_evictable); // Debugging statement
        } else {
            println!("Frame {} not found in frame_store", frame_id); // Debugging statement
            frame_store.insert(frame_id, Frame {
                is_evictable: set_evictable,
                access_times: vec![],
                k: self.k,
                fid: frame_id,
            });
        }
    }
    pub fn remove(&self, frame_id: FrameId) {
        let mut frame_store = self.frame_store.lock().unwrap();
        if let Some(frame) = frame_store.get(&frame_id) {
            if frame.is_evictable {
                frame_store.remove(&frame_id);
                println!("Removed frame {}", frame_id); // Debugging statement
            } else {
                panic!("Attempt to remove a non-evictable frame {}", frame_id);
            }
        } else {
            println!("Attempt to remove a non-existing frame {}", frame_id); // Debugging statement
        }
    }

    pub fn size(&self) -> usize {
        let frame_store = self.frame_store.lock().unwrap();
        frame_store
            .iter()
            .filter(|&frame| frame.1.is_evictable)
            .count()
    }

    fn current_time() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
