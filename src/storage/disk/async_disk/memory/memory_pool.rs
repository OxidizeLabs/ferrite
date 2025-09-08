//! Memory management utilities for the Async Disk Manager
//! 
//! This module contains memory management utilities, including NUMA-aware allocation.

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicUsize, Ordering};

/// NUMA-aware memory allocator for high-performance scenarios
#[derive(Debug)]
pub struct NumaAllocator {
    #[allow(dead_code)]
    node_id: usize,
    allocated_bytes: AtomicUsize,
}

impl NumaAllocator {
    pub fn new(node_id: usize) -> Self {
        Self {
            node_id,
            allocated_bytes: AtomicUsize::new(0),
        }
    }

    pub fn allocated_bytes(&self) -> usize {
        self.allocated_bytes.load(Ordering::Relaxed)
    }
}

unsafe impl GlobalAlloc for NumaAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // In a real implementation, this would use NUMA-specific allocation
        // For now, use standard allocator but track bytes
        unsafe {
            let ptr = std::alloc::System.alloc(layout);
            if !ptr.is_null() {
                self.allocated_bytes.fetch_add(layout.size(), Ordering::Relaxed);
            }
            ptr
        }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            std::alloc::System.dealloc(ptr, layout);
            self.allocated_bytes.fetch_sub(layout.size(), Ordering::Relaxed);
        }
    }
}

/// Memory pool for efficient allocation of database pages
#[derive(Debug)]
pub struct MemoryPool {
    pool_size_mb: usize,
    allocated_bytes: AtomicUsize,
    #[allow(dead_code)]
    numa_aware: bool,
    #[allow(dead_code)]
    numa_node: Option<usize>,
}

impl MemoryPool {
    pub fn new(pool_size_mb: usize, numa_aware: bool, numa_node: Option<usize>) -> Self {
        Self {
            pool_size_mb,
            allocated_bytes: AtomicUsize::new(0),
            numa_aware,
            numa_node,
        }
    }

    pub fn allocated_bytes(&self) -> usize {
        self.allocated_bytes.load(Ordering::Relaxed)
    }

    pub fn total_size_bytes(&self) -> usize {
        self.pool_size_mb * 1024 * 1024
    }

    pub fn available_bytes(&self) -> usize {
        self.total_size_bytes().saturating_sub(self.allocated_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_allocator() {
        let allocator = NumaAllocator::new(0);
        assert_eq!(allocator.allocated_bytes(), 0);
        
        // We can't easily test the actual allocation without using it as a global allocator
        // This is just a basic test of the API
    }

    #[test]
    fn test_memory_pool() {
        let pool = MemoryPool::new(128, false, None);
        assert_eq!(pool.allocated_bytes(), 0);
        assert_eq!(pool.total_size_bytes(), 128 * 1024 * 1024);
        assert_eq!(pool.available_bytes(), 128 * 1024 * 1024);
    }
}