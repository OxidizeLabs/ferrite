use std::sync::atomic::{AtomicBool, Ordering};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};
use std::hint;

pub struct Spinlock<T: ?Sized> {
    lock: AtomicBool,
    data: UnsafeCell<T>,
}

unsafe impl<T: ?Sized + Send> Send for Spinlock<T> {}
unsafe impl<T: ?Sized + Send> Sync for Spinlock<T> {}

impl<T> Spinlock<T> {
    pub fn new(data: T) -> Spinlock<T> {
        Spinlock {
            lock: AtomicBool::new(false),
            data: UnsafeCell::new(data),
        }
    }

    pub fn lock(&self) -> SpinlockGuard<T> {
        while self.lock.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_err() {
            hint::spin_loop();
        }
        SpinlockGuard { spinlock: self }
    }

    pub fn try_lock(&self) -> Option<SpinlockGuard<T>> {
        if self.lock.compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed).is_ok() {
            Some(SpinlockGuard { spinlock: self })
        } else {
            None
        }
    }

    fn unlock(&self) {
        self.lock.store(false, Ordering::Release);
    }
}

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
