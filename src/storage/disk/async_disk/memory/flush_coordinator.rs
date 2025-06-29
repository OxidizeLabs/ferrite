//! Flush coordination and timing management
//! 
//! This module handles flush operations coordination, timing decisions,
//! and flush policy enforcement.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Coordinates flush operations and timing
#[derive(Debug)]
pub struct FlushCoordinator {
    flush_in_progress: AtomicBool,
    flush_threshold: usize,
    flush_interval: Duration,
    last_flush: Mutex<Instant>,
}

/// Flush decision result
#[derive(Debug, Clone, PartialEq)]
pub enum FlushDecision {
    /// No flush needed
    NoFlush,
    /// Flush due to page count threshold
    ThresholdFlush,
    /// Flush due to time interval
    IntervalFlush,
    /// Forced flush requested
    ForceFlush,
}

impl FlushCoordinator {
    /// Creates a new flush coordinator with the specified configuration
    pub fn new(flush_threshold: usize, flush_interval: Duration) -> Self {
        Self {
            flush_in_progress: AtomicBool::new(false),
            flush_threshold,
            flush_interval,
            last_flush: Mutex::new(Instant::now()),
        }
    }

    /// Checks if a flush should be performed based on current state
    pub async fn should_flush(&self, dirty_pages: usize, time_since_last_flush: Duration) -> FlushDecision {
        // Check if we've exceeded the flush threshold
        if dirty_pages >= self.flush_threshold {
            return FlushDecision::ThresholdFlush;
        }
        
        // Check if we've exceeded the flush interval
        if time_since_last_flush >= self.flush_interval {
            return FlushDecision::IntervalFlush;
        }
        
        FlushDecision::NoFlush
    }

    /// Attempts to start a flush operation
    /// Returns true if flush can proceed, false if already in progress
    pub fn try_start_flush(&self) -> bool {
        self.flush_in_progress
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Completes a flush operation and updates timing
    pub async fn complete_flush(&self) {
        // Update last flush time
        *self.last_flush.lock().await = Instant::now();
        
        // Clear flush in progress flag
        self.flush_in_progress.store(false, Ordering::SeqCst);
    }

    /// Checks if a flush is currently in progress
    pub fn is_flush_in_progress(&self) -> bool {
        self.flush_in_progress.load(Ordering::SeqCst)
    }

    /// Gets the time since the last flush
    pub async fn time_since_last_flush(&self) -> Duration {
        self.last_flush.lock().await.elapsed()
    }

    /// Gets the flush threshold (number of dirty pages)
    pub fn flush_threshold(&self) -> usize {
        self.flush_threshold
    }

    /// Gets the flush interval duration
    pub fn flush_interval(&self) -> Duration {
        self.flush_interval
    }

    /// Updates the flush threshold
    pub fn set_flush_threshold(&mut self, threshold: usize) {
        self.flush_threshold = threshold;
    }

    /// Updates the flush interval
    pub fn set_flush_interval(&mut self, interval: Duration) {
        self.flush_interval = interval;
    }

    /// Forces completion of any in-progress flush (for shutdown scenarios)
    pub fn force_complete(&self) {
        self.flush_in_progress.store(false, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[tokio::test]
    async fn test_flush_coordinator_creation() {
        let coordinator = FlushCoordinator::new(100, Duration::from_millis(1000));
        assert_eq!(coordinator.flush_threshold(), 100);
        assert_eq!(coordinator.flush_interval(), Duration::from_millis(1000));
        assert!(!coordinator.is_flush_in_progress());
    }

    #[tokio::test]
    async fn test_should_flush_threshold() {
        let coordinator = FlushCoordinator::new(10, Duration::from_secs(60));
        
        // Below threshold
        let decision = coordinator.should_flush(5, Duration::from_secs(1)).await;
        assert_eq!(decision, FlushDecision::NoFlush);
        
        // At threshold
        let decision = coordinator.should_flush(10, Duration::from_secs(1)).await;
        assert_eq!(decision, FlushDecision::ThresholdFlush);
        
        // Above threshold
        let decision = coordinator.should_flush(15, Duration::from_secs(1)).await;
        assert_eq!(decision, FlushDecision::ThresholdFlush);
    }

    #[tokio::test]
    async fn test_should_flush_interval() {
        let coordinator = FlushCoordinator::new(100, Duration::from_millis(100));
        
        // Within interval
        let decision = coordinator.should_flush(5, Duration::from_millis(50)).await;
        assert_eq!(decision, FlushDecision::NoFlush);
        
        // At interval
        let decision = coordinator.should_flush(5, Duration::from_millis(100)).await;
        assert_eq!(decision, FlushDecision::IntervalFlush);
        
        // Past interval
        let decision = coordinator.should_flush(5, Duration::from_millis(150)).await;
        assert_eq!(decision, FlushDecision::IntervalFlush);
    }

    #[tokio::test]
    async fn test_flush_coordination() {
        let coordinator = FlushCoordinator::new(10, Duration::from_secs(60));
        
        // Should be able to start flush
        assert!(coordinator.try_start_flush());
        assert!(coordinator.is_flush_in_progress());
        
        // Should not be able to start another flush
        assert!(!coordinator.try_start_flush());
        assert!(coordinator.is_flush_in_progress());
        
        // Complete flush
        coordinator.complete_flush().await;
        assert!(!coordinator.is_flush_in_progress());
        
        // Should be able to start flush again
        assert!(coordinator.try_start_flush());
    }

    #[tokio::test]
    async fn test_time_since_last_flush() {
        let coordinator = FlushCoordinator::new(10, Duration::from_secs(60));
        
        // Time should be minimal right after creation
        let time1 = coordinator.time_since_last_flush().await;
        assert!(time1 < Duration::from_millis(100));
        
        // Wait a bit
        sleep(Duration::from_millis(50)).await;
        
        let time2 = coordinator.time_since_last_flush().await;
        assert!(time2 > time1);
        assert!(time2 >= Duration::from_millis(40)); // Allow some tolerance
    }

    #[tokio::test]
    async fn test_configuration_updates() {
        let mut coordinator = FlushCoordinator::new(10, Duration::from_secs(60));
        
        coordinator.set_flush_threshold(20);
        assert_eq!(coordinator.flush_threshold(), 20);
        
        coordinator.set_flush_interval(Duration::from_secs(30));
        assert_eq!(coordinator.flush_interval(), Duration::from_secs(30));
    }

    #[tokio::test]
    async fn test_force_complete() {
        let coordinator = FlushCoordinator::new(10, Duration::from_secs(60));
        
        // Start flush
        assert!(coordinator.try_start_flush());
        assert!(coordinator.is_flush_in_progress());
        
        // Force complete
        coordinator.force_complete();
        assert!(!coordinator.is_flush_in_progress());
        
        // Should be able to start again
        assert!(coordinator.try_start_flush());
    }

    #[tokio::test]
    async fn test_complete_flush_updates_time() {
        let coordinator = FlushCoordinator::new(10, Duration::from_secs(60));
        
        // Wait a bit first
        sleep(Duration::from_millis(100)).await;
        let time_before = coordinator.time_since_last_flush().await;
        assert!(time_before >= Duration::from_millis(90));
        
        // Complete flush (this updates last flush time)
        coordinator.complete_flush().await;
        
        let time_after = coordinator.time_since_last_flush().await;
        assert!(time_after < time_before);
        assert!(time_after < Duration::from_millis(10));
    }
} 