//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// rwmutex.rs
//
// Identification: src/common/rwlatch.rs
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

use std::sync::{RwLock, RwLockWriteGuard, RwLockReadGuard};

pub struct ReaderWriterLatch {
    mutex: RwLock<()>,
}

impl ReaderWriterLatch {
    pub fn new() -> Self {
        ReaderWriterLatch {
            mutex: RwLock::new(()),
        }
    }

    /**
     * Acquire a write latch.
     */
    pub fn w_lock(&self) -> RwLockWriteGuard<()> {
        self.mutex.write().unwrap()
    }

    /**
     * Release a write latch.
     */
    pub fn w_unlock(&self, guard: RwLockWriteGuard<()>) {
        drop(guard);
    }

    /**
     * Acquire a read latch.
     */
    pub fn r_lock(&self) -> RwLockReadGuard<()> {
        self.mutex.read().unwrap()
    }

    /**
     * Release a read latch.
     */
    pub fn r_unlock(&self, guard: RwLockReadGuard<()>) {
        drop(guard);
    }
}
