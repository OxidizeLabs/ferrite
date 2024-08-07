use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::storage::disk::disk_manager::DiskIO;
use async_trait::async_trait;
use log::info;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::ThreadId;
use std::time::Duration;

type Page = [u8; DB_PAGE_SIZE];

/// DiskManagerMemory replicates the utility of DiskManager on memory.
/// It is primarily used for data structure performance testing.
pub struct DiskManagerMemory {
    memory: RwLock<Vec<u8>>,
}

impl DiskManagerMemory {
    pub fn new(num_pages: usize) -> Self {
        Self {
            memory: RwLock::new(vec![0; num_pages * DB_PAGE_SIZE]),
        }
    }
}

#[async_trait]
impl DiskIO for DiskManagerMemory {
    async fn write_page(&self, page_id: PageId, page_data: &[u8; 4096]) {
        let offset = page_id as usize * DB_PAGE_SIZE;
        info!("Writing page {} at offset {}", page_id, offset);
        info!("Page data being written: {:?}", &page_data[..64]); // Debugging statement

        let mut memory = self.memory.write().unwrap();
        memory[offset..offset + DB_PAGE_SIZE].copy_from_slice(page_data);
    }

    async fn read_page(&self, page_id: PageId, page_data: &mut [u8; 4096]) {
        let offset = page_id as usize * DB_PAGE_SIZE;
        info!("Reading page {} at offset {}", page_id, offset); // Debugging statement

        let memory = self.memory.read().unwrap();
        page_data.copy_from_slice(&memory[offset..offset + DB_PAGE_SIZE]);
    }
}

struct ProtectedPage {
    page: Mutex<Page>,
    rwlock: RwLock<()>,
}

/// DiskManagerUnlimitedMemory replicates the utility of DiskManager on memory.
/// It is primarily used for data structure performance testing.
pub struct DiskManagerUnlimitedMemory {
    data: RwLock<HashMap<PageId, Arc<ProtectedPage>>>,
    latency_simulator_enabled: AtomicBool,
    recent_access: Mutex<Vec<PageId>>,
    access_ptr: AtomicUsize,
    thread_id: Mutex<Option<thread::ThreadId>>,
}

impl DiskManagerUnlimitedMemory {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            latency_simulator_enabled: AtomicBool::new(false),
            recent_access: Mutex::new(vec![-1; 4]),
            access_ptr: AtomicUsize::new(0),
            thread_id: Mutex::new(None),
        }
    }

    fn process_latency(&self, page_id: PageId) {
        let mut sleep_micro_sec = 1000; // for random access, 1ms latency
        if self.latency_simulator_enabled.load(Ordering::Relaxed) {
            let recent_access = self.recent_access.lock().unwrap();
            for &recent_page_id in recent_access.iter() {
                if (recent_page_id & (!0x3)) == (page_id & (!0x3)) {
                    sleep_micro_sec = 100; // for access in the same "block", 0.1ms latency
                    break;
                }
                if page_id >= recent_page_id && page_id <= recent_page_id + 3 {
                    sleep_micro_sec = 100; // for sequential access, 0.1ms latency
                    break;
                }
            }
            drop(recent_access);
            thread::sleep(Duration::from_micros(sleep_micro_sec));
        }
    }

    fn post_process_latency(&self, page_id: PageId) {
        if self.latency_simulator_enabled.load(Ordering::Relaxed) {
            let mut recent_access = self.recent_access.lock().unwrap();
            let access_ptr = self.access_ptr.load(Ordering::Relaxed);
            recent_access[access_ptr] = page_id;
            self.access_ptr
                .store((access_ptr + 1) % recent_access.len(), Ordering::Relaxed);
        }
    }

    pub fn enable_latency_simulator(&self, enabled: bool) {
        self.latency_simulator_enabled
            .store(enabled, Ordering::Relaxed);
    }

    pub fn get_last_read_thread_and_clear(&self) -> Option<thread::ThreadId> {
        let mut thread_id = self.thread_id.lock().unwrap();
        let t = *thread_id;
        *thread_id = None;
        t
    }
}

#[async_trait]
impl DiskIO for DiskManagerUnlimitedMemory {
    async fn write_page(&self, page_id: PageId, page_data: &[u8; 4096]) {
        self.process_latency(page_id);

        let mut data = self.data.write().unwrap();
        let page = data
            .entry(page_id)
            .or_insert_with(|| {
                Arc::new(ProtectedPage {
                    page: Mutex::new([0; DB_PAGE_SIZE]),
                    rwlock: RwLock::new(()),
                })
            })
            .clone();

        drop(data);

        let mut page_lock = page.page.lock().unwrap();
        page_lock.copy_from_slice(&*page_data);

        self.post_process_latency(page_id);
    }

    async fn read_page(&self, page_id: PageId, page_data: &mut [u8; 4096]) {
        self.process_latency(page_id);

        let data = self.data.read().unwrap();
        let page = data.get(&page_id).expect("page not found");

        let page_lock = page.page.lock().unwrap();
        page_data.copy_from_slice(&page_lock.clone());

        self.post_process_latency(page_id);
    }
}
