use crate::common::config::{PageId, DB_PAGE_SIZE};
use crate::storage::disk::disk_manager::DiskIO;
use log::info;
use std::collections::HashMap;
use std::io::{Error, ErrorKind, Result as IoResult};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::thread::ThreadId;
use std::time::Duration;

type Page = [u8; DB_PAGE_SIZE as usize];

/// DiskManagerMemory replicates the utility of DiskManager on memory.
/// It is primarily used for data structure performance testing.
pub struct DiskManagerMemory {
    memory: RwLock<Vec<u8>>,
}

impl DiskManagerMemory {
    pub fn new(num_pages: usize) -> Self {
        Self {
            memory: RwLock::new(vec![0; num_pages * DB_PAGE_SIZE as usize]),
        }
    }
}

impl DiskIO for DiskManagerMemory {
    fn write_page(&self, page_id: PageId, page_data: &[u8; DB_PAGE_SIZE as usize]) -> IoResult<()> {
        let offset = (page_id as usize) * (DB_PAGE_SIZE as usize);
        info!("Writing page {} at offset {}", page_id, offset);

        let mut memory = self
            .memory
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Failed to acquire write lock for memory"))?;
        memory[offset..offset + DB_PAGE_SIZE as usize].copy_from_slice(page_data);

        Ok(())
    }

    fn read_page(
        &self,
        page_id: PageId,
        page_data: &mut [u8; DB_PAGE_SIZE as usize],
    ) -> IoResult<()> {
        let offset = (page_id as usize) * (DB_PAGE_SIZE as usize);
        info!("Reading page {} at offset {}", page_id, offset);

        let memory = self
            .memory
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Failed to acquire read lock for memory"))?;
        if offset + DB_PAGE_SIZE as usize <= memory.len() {
            page_data.copy_from_slice(&memory[offset..offset + DB_PAGE_SIZE as usize]);
            Ok(())
        } else {
            Err(Error::new(
                ErrorKind::UnexpectedEof,
                "Page data exceeds memory bounds",
            ))
        }
    }

    fn write_page_with_retry(&self, page_id: PageId, page_data: &[u8; DB_PAGE_SIZE as usize]) -> IoResult<()> {
        todo!()
    }

    fn read_page_with_retry(&self, page_id: PageId, page_data: &mut [u8; DB_PAGE_SIZE as usize]) -> IoResult<()> {
        todo!()
    }
}

/// DiskManagerUnlimitedMemory replicates the utility of DiskManager on memory.
/// It is primarily used for data structure performance testing.
pub struct DiskManagerUnlimitedMemory {
    data: RwLock<HashMap<PageId, Arc<ProtectedPage>>>,
    latency_simulator_enabled: AtomicBool,
    recent_access: Mutex<Vec<PageId>>,
    access_ptr: AtomicUsize,
    thread_id: Mutex<Option<ThreadId>>,
}

struct ProtectedPage {
    page: Mutex<Page>,
}

impl DiskManagerUnlimitedMemory {
    pub fn new() -> Self {
        Self {
            data: RwLock::new(HashMap::new()),
            latency_simulator_enabled: AtomicBool::new(false),
            recent_access: Mutex::new(vec![u64::MAX; 4]),
            access_ptr: AtomicUsize::new(0),
            thread_id: Mutex::new(None),
        }
    }

    fn process_latency(&self, page_id: PageId) {
        if !self.latency_simulator_enabled.load(Ordering::Relaxed) {
            return;
        }

        let mut sleep_micro_sec = 1000; // Default for random access
        let recent_access = self.recent_access.lock().unwrap();

        for &recent_page_id in recent_access.iter() {
            if (recent_page_id & (!0x3)) == (page_id & (!0x3))
                || (page_id >= recent_page_id && page_id <= recent_page_id + 3)
            {
                sleep_micro_sec = 100; // Lower latency for sequential or block access
                break;
            }
        }

        drop(recent_access);
        thread::sleep(Duration::from_micros(sleep_micro_sec));
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

    pub fn get_last_read_thread_and_clear(&self) -> Option<ThreadId> {
        let mut thread_id = self.thread_id.lock().unwrap();
        let t = *thread_id;
        *thread_id = None;
        t
    }
}

impl DiskIO for DiskManagerUnlimitedMemory {
    fn write_page(
        &self,
        page_id: PageId,
        page_data: &[u8; DB_PAGE_SIZE as usize],
    ) -> Result<(), Error> {
        self.process_latency(page_id);

        let mut data = self
            .data
            .write()
            .map_err(|_| Error::new(ErrorKind::Other, "Failed to acquire write lock for data"))?;

        let page = data
            .entry(page_id)
            .or_insert_with(|| {
                Arc::new(ProtectedPage {
                    page: Mutex::new([0; DB_PAGE_SIZE as usize]),
                })
            })
            .clone();

        drop(data); // Release the lock early

        let mut page_lock = page
            .page
            .lock()
            .map_err(|_| Error::new(ErrorKind::Other, "Failed to acquire page lock"))?;
        page_lock.copy_from_slice(page_data);

        self.post_process_latency(page_id);
        Ok(())
    }

    fn read_page(
        &self,
        page_id: PageId,
        page_data: &mut [u8; DB_PAGE_SIZE as usize],
    ) -> Result<(), Error> {
        self.process_latency(page_id);

        let data = self
            .data
            .read()
            .map_err(|_| Error::new(ErrorKind::Other, "Failed to acquire read lock for data"))?;

        let page = data
            .get(&page_id)
            .ok_or_else(|| Error::new(ErrorKind::NotFound, "Page not found"))?
            .clone();

        let page_lock = page
            .page
            .lock()
            .map_err(|_| Error::new(ErrorKind::Other, "Failed to acquire page lock"))?;

        // Dereference the `MutexGuard` to get the actual slice.
        page_data.copy_from_slice(&*page_lock);

        self.post_process_latency(page_id);
        Ok(())
    }

    fn write_page_with_retry(&self, page_id: PageId, page_data: &[u8; DB_PAGE_SIZE as usize]) -> IoResult<()> {
        todo!()
    }

    fn read_page_with_retry(&self, page_id: PageId, page_data: &mut [u8; DB_PAGE_SIZE as usize]) -> IoResult<()> {
        todo!()
    }
}
