use std::sync::atomic::{AtomicUsize, Ordering};
use std::hint;
use crate::common::spinlock::{Spinlock, SpinlockGuard};

/// A reader-writer latch implemented using spinlocks for thread synchronization.
/// This allows multiple readers to access the shared resource simultaneously,
/// but only one writer at a time, with readers and writers mutually excluding each other.
pub struct ReaderWriterLatch {
    /// The count of active readers.
    reader_count: AtomicUsize,
    /// The spinlock used to control access for writers.
    writer_lock: Spinlock<()>,
}

impl ReaderWriterLatch {
    /// Creates a new `ReaderWriterLatch`.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::common::rwlatch::ReaderWriterLatch;
    /// let latch = ReaderWriterLatch::new();
    /// ```
    pub fn new() -> Self {
        ReaderWriterLatch {
            reader_count: AtomicUsize::new(0),
            writer_lock: Spinlock::new(()),
        }
    }

    /// Acquires the write latch, blocking until it is available.
    ///
    /// This ensures exclusive access to the shared resource, preventing any readers or other writers.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::common::rwlatch::ReaderWriterLatch;
    /// let latch = ReaderWriterLatch::new();
    /// let write_guard = latch.w_lock();
    /// // Critical section for writing
    /// latch.w_unlock(write_guard);
    /// ```
    pub fn w_lock(&self) -> SpinlockGuard<()> {
        self.writer_lock.lock()
    }

    /// Releases the write latch.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::common::rwlatch::ReaderWriterLatch;
    /// let latch = ReaderWriterLatch::new();
    /// let write_guard = latch.w_lock();
    /// // Critical section for writing
    /// latch.w_unlock(write_guard);
    /// ```
    pub fn w_unlock(&self, guard: SpinlockGuard<()>) {
        drop(guard);
    }

    /// Acquires the read latch, blocking until it is available.
    ///
    /// This allows multiple readers to access the shared resource simultaneously,
    /// but blocks if there is an active writer.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::common::rwlatch::ReaderWriterLatch;
    /// let latch = ReaderWriterLatch::new();
    /// latch.r_lock();
    /// // Critical section for reading
    /// latch.r_unlock();
    /// ```
    pub fn r_lock(&self) {
        loop {
            // Wait until no writers hold the lock
            while self.writer_lock.lock.load(Ordering::Acquire) {
                hint::spin_loop();
            }

            // Increment the reader count
            self.reader_count.fetch_add(1, Ordering::Acquire);

            // Check again for writers
            if !self.writer_lock.lock.load(Ordering::Acquire) {
                break;
            }

            // If a writer has locked, decrement the reader count and retry
            self.reader_count.fetch_sub(1, Ordering::Release);
        }
    }

    /// Releases the read latch.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::common::rwlatch::ReaderWriterLatch;
    /// let latch = ReaderWriterLatch::new();
    /// latch.r_lock();
    /// // Critical section for reading
    /// latch.r_unlock();
    /// ```
    pub fn r_unlock(&self) {
        self.reader_count.fetch_sub(1, Ordering::Release);
    }
}

fn main() {
    // Example usage
    let latch = ReaderWriterLatch::new();

    // Write lock
    let write_guard = latch.w_lock();
    // Critical section for writing
    latch.w_unlock(write_guard);

    // Read lock
    latch.r_lock();
    // Critical section for reading
    latch.r_unlock();
}
