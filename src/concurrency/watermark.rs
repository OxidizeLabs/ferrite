use crate::common::config::Timestamp;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

/// Tracks all read timestamps.
#[derive(Debug)]
pub struct Watermark {
    /// Set of active transaction timestamps
    active_txns: HashSet<Timestamp>,
    /// Current watermark value
    watermark: Timestamp,
    /// Next timestamp to assign
    next_ts: AtomicU64,
}

impl Watermark {
    /// Creates a new Watermark with the given commit timestamp.
    pub fn new() -> Self {
        let next_ts = AtomicU64::new(1); // Start from 1
        Self {
            active_txns: HashSet::new(),
            watermark: 1, // Initialize watermark to 1 since that's our first available timestamp
            next_ts,
        }
    }

    /// Gets the next available timestamp
    pub fn get_next_ts(&self) -> Timestamp {
        self.next_ts.fetch_add(1, Ordering::SeqCst)
    }

    /// Adds a transaction timestamp to the active set
    pub fn add_txn(&mut self, ts: Timestamp) {
        self.active_txns.insert(ts);
        self.update_watermark();
    }

    /// Removes a transaction timestamp from the active set
    pub fn remove_txn(&mut self, ts: Timestamp) {
        self.active_txns.remove(&ts);
        self.update_watermark();
    }

    /// Updates the commit timestamp for a transaction
    pub fn update_commit_ts(&mut self, ts: Timestamp) {
        self.watermark = self.next_ts.swap(ts, Ordering::SeqCst);
    }

    /// Gets the current watermark value
    pub fn get_watermark(&self) -> Timestamp {
        self.watermark
    }

    /// Updates the watermark based on active transactions
    fn update_watermark(&mut self) {
        if self.active_txns.is_empty() {
            self.watermark = self.next_ts.load(Ordering::SeqCst);
        } else {
            self.watermark = *self.active_txns.iter().min().unwrap();
        }
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Watermark::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_basic() {
        let mut watermark = Watermark::new();

        // Test timestamp generation is sequential starting from 1
        let ts1 = watermark.get_next_ts();
        let ts2 = watermark.get_next_ts();
        assert_eq!(ts1, 1);
        assert_eq!(ts2, 2);

        // Test transaction tracking
        watermark.add_txn(ts1);
        assert_eq!(watermark.get_watermark(), ts1);

        watermark.add_txn(ts2);
        assert_eq!(watermark.get_watermark(), ts1); // Should be minimum active ts

        watermark.remove_txn(ts1);
        assert_eq!(watermark.get_watermark(), ts2);

        watermark.remove_txn(ts2);
        assert_eq!(watermark.get_watermark(), 3); // Next available ts when no active txns
    }

    #[test]
    fn test_watermark_empty() {
        let mut watermark = Watermark::new();

        // Initially, with no transactions, watermark should be next available ts
        assert_eq!(watermark.get_watermark(), 1);

        let ts = watermark.get_next_ts(); // Should be 1
        assert_eq!(ts, 1);

        watermark.add_txn(ts);
        assert_eq!(watermark.get_watermark(), ts);

        watermark.remove_txn(ts);
        assert_eq!(watermark.get_watermark(), 2); // Next available ts
    }

    #[test]
    fn test_watermark_multiple_transactions() {
        let mut watermark = Watermark::new();

        // Get sequential timestamps
        let ts1 = watermark.get_next_ts(); // 1
        let ts2 = watermark.get_next_ts(); // 2
        let ts3 = watermark.get_next_ts(); // 3

        assert_eq!(ts1, 1);
        assert_eq!(ts2, 2);
        assert_eq!(ts3, 3);

        // Add transactions in different order than timestamp sequence
        watermark.add_txn(ts2);
        watermark.add_txn(ts3);
        watermark.add_txn(ts1);

        // Watermark should always be the minimum active timestamp
        assert_eq!(watermark.get_watermark(), ts1);

        watermark.remove_txn(ts1);
        assert_eq!(watermark.get_watermark(), ts2);

        watermark.remove_txn(ts2);
        assert_eq!(watermark.get_watermark(), ts3);

        watermark.remove_txn(ts3);
        assert_eq!(watermark.get_watermark(), 4); // Next available ts
    }
}
