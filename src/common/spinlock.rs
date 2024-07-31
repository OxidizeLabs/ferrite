use std::cell::UnsafeCell;
use std::hint;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, Ordering};

/// A simple spinlock for synchronizing access to shared data.
pub struct Spinlock<T: ?Sized> {
    /// The atomic boolean used to indicate whether the lock is held.
    pub(crate) lock: AtomicBool,
    /// The data protected by the spinlock.
    data: UnsafeCell<T>,
}

// Implement Send and Sync for Spinlock
unsafe impl<T: ?Sized + Send> Send for Spinlock<T> {}
unsafe impl<T: ?Sized + Send> Sync for Spinlock<T> {}

impl<T: ?Sized> Spinlock<T> {
    /// Creates a new `Spinlock` wrapping the provided data.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::common::spinlock::Spinlock;
    /// let spinlock = Spinlock::new(5);
    /// ```
    pub fn new(data: T) -> Spinlock<T>
    where
        T: Sized,
    {
        Spinlock {
            lock: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }

    /// Acquires the lock, blocking the current thread until it is able to do so.
    ///
    /// Returns a guard that releases the lock when dropped.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::common::spinlock::Spinlock;
    /// let spinlock = Spinlock::new(5);
    /// let guard = spinlock.lock();
    /// ```
    pub fn lock(&self) -> SpinlockGuard<T> {
        while self.lock.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            hint::spin_loop();
        }
        SpinlockGuard { spinlock: self }
    }

    /// Tries to acquire the lock without blocking. If the lock is currently held, returns `None`.
    ///
    /// Returns `Some` containing a guard that releases the lock when dropped if the lock was successfully acquired.
    ///
    /// # Examples
    ///
    /// ```
    /// use tkdb::common::spinlock::Spinlock;
    /// let spinlock = Spinlock::new(5);
    /// if let Some(guard) = spinlock.try_lock() {
    ///     // lock acquired
    /// } else {
    ///     // lock not acquired
    /// };
    /// ```
    pub fn try_lock(&self) -> Option<SpinlockGuard<T>> {
        if self.lock.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
            Some(SpinlockGuard { spinlock: self })
        } else {
            None
        }
    }

    /// Releases the lock.
    fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }
}

/// A guard that releases the spinlock when dropped.
pub struct SpinlockGuard<'a, T: ?Sized + 'a> {
    spinlock: &'a Spinlock<T>,
}

impl<'a, T: ?Sized> Deref for SpinlockGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        // Safe because we hold the lock
        unsafe { &*self.spinlock.data.get() }
    }
}

impl<'a, T: ?Sized> DerefMut for SpinlockGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        // Safe because we hold the lock
        unsafe { &mut *self.spinlock.data.get() }
    }
}

impl<'a, T: ?Sized> Drop for SpinlockGuard<'a, T> {
    fn drop(&mut self) {
        self.spinlock.unlock();
    }
}

impl Spinlock<()> {
    /// Check if the spinlock is currently locked.
    pub fn is_locked(&self) -> bool {
        self.lock.load(Ordering::Acquire)
    }
}
