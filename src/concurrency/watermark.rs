use crate::common::config::Timestamp;
use std::collections::HashMap;

/// Tracks all read timestamps.
#[derive(Debug)]
pub struct Watermark {
    commit_ts: Timestamp,
    watermark: Timestamp,
    current_reads: HashMap<Timestamp, i64>,
}

impl Watermark {
    /// Creates a new Watermark with the given commit timestamp.
    pub fn new(commit_ts: Timestamp) -> Self {
        Self {
            commit_ts,
            watermark: commit_ts,
            current_reads: HashMap::new(),
        }
    }

    /// Adds a transaction with the given read timestamp.
    pub fn add_txn(&mut self, read_ts: Timestamp) {
        *self.current_reads.entry(read_ts).or_insert(0) += 1;
        self.watermark = read_ts.min(self.watermark);
    }

    /// Removes a transaction with the given read timestamp.
    pub fn remove_txn(&mut self, read_ts: Timestamp) {
        if let Some(count) = self.current_reads.get_mut(&read_ts) {
            *count -= 1;
            if *count <= 0 {
                self.current_reads.remove(&read_ts);
                // Recalculate watermark if necessary
                if !self.current_reads.is_empty() {
                    self.watermark = *self.current_reads.keys().min().unwrap();
                }
            }
        }
    }

    /// Updates the commit timestamp.
    ///
    /// The caller should update commit timestamp before removing the transaction
    /// from the watermark to track watermark correctly.
    pub fn update_commit_ts(&mut self, commit_ts: Timestamp) {
        self.commit_ts = commit_ts;
    }

    /// Gets the current watermark.
    pub fn get_watermark(&self) -> Timestamp {
        if self.current_reads.is_empty() {
            self.commit_ts
        } else {
            self.watermark
        }
    }
}

impl Default for Watermark {
    fn default() -> Self {
        Watermark::new(Timestamp::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_watermark_basic() {
        let mut wm = Watermark::new(100);
        assert_eq!(wm.get_watermark(), 100);

        wm.add_txn(90);
        assert_eq!(wm.get_watermark(), 90);

        wm.add_txn(95);
        assert_eq!(wm.get_watermark(), 90);

        wm.remove_txn(90);
        assert_eq!(wm.get_watermark(), 95);

        wm.remove_txn(95);
        assert_eq!(wm.get_watermark(), 100);
    }

    #[test]
    fn test_watermark_multiple_same_timestamp() {
        let mut wm = Watermark::new(100);

        wm.add_txn(90);
        wm.add_txn(90);
        assert_eq!(wm.get_watermark(), 90);

        wm.remove_txn(90);
        assert_eq!(wm.get_watermark(), 90);

        wm.remove_txn(90);
        assert_eq!(wm.get_watermark(), 100);
    }
}
