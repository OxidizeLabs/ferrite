//! Machine learning-based prefetching for the Async Disk Manager
//! 
//! This module contains the ML-based prefetcher for predicting page access patterns.

use crate::common::config::PageId;
use std::collections::{VecDeque, HashMap};
use std::time::Instant;

/// Advanced machine learning-based prefetcher
#[derive(Debug)]
pub struct MLPrefetcher {
    access_history: VecDeque<(PageId, Instant)>,
    pattern_weights: HashMap<Vec<PageId>, f64>,
    prediction_accuracy: f64,
    learning_rate: f64,
    min_pattern_length: usize,
    max_pattern_length: usize,
    prefetch_distance: usize,
    last_predictions: Option<Vec<PageId>>,
}

impl Default for MLPrefetcher {
    fn default() -> Self {
        Self::new()
    }
}

impl MLPrefetcher {
    pub fn new() -> Self {
        Self {
            access_history: VecDeque::with_capacity(1000),
            pattern_weights: HashMap::new(),
            prediction_accuracy: 0.0,
            learning_rate: 0.1,
            min_pattern_length: 2,
            max_pattern_length: 8,
            prefetch_distance: 4,
            last_predictions: None,
        }
    }

    /// Records a page access and updates learning model
    pub fn record_access(&mut self, page_id: PageId) {
        let now = Instant::now();
        self.access_history.push_back((page_id, now));

        // Update prediction accuracy based on last predictions
        if let Some(preds) = &self.last_predictions {
            let hit = preds.contains(&page_id);
            let correctness = if hit { 1.0 } else { 0.0 };
            self.prediction_accuracy =
                self.prediction_accuracy * (1.0 - self.learning_rate) + correctness * self.learning_rate;
        }

        // Maintain history size
        while self.access_history.len() > 1000 {
            self.access_history.pop_front();
        }

        // Update pattern weights
        self.update_patterns();
    }

    /// Predicts next pages to prefetch based on learned patterns
    pub fn predict_prefetch(&self, current_page: PageId) -> Vec<PageId> {
        let mut predictions = Vec::new();

        // Find patterns ending with current page
        for (pattern, weight) in &self.pattern_weights {
            if let Some(&last_page) = pattern.last()
                && last_page == current_page && *weight > 0.5 {
                    // Predict next pages based on this pattern
                    for i in 1..=self.prefetch_distance {
                        if let Some(history_entry) = self.find_pattern_continuation(pattern, i) {
                            predictions.push(history_entry);
                        }
                    }
                }
        }

        // Remove duplicates and limit predictions
        let mut unique = predictions;
        unique.sort_unstable();
        unique.dedup();
        unique.truncate(8); // Limit prefetch size

        unique
    }

    /// Updates pattern weights based on recent access history
    fn update_patterns(&mut self) {
        if self.access_history.len() < self.min_pattern_length {
            return;
        }

        // Build recent access list in chronological order (oldest -> newest)
        let mut recent_accesses: Vec<PageId> = self.access_history
            .iter()
            .rev()
            .take(20)
            .map(|(page_id, _)| *page_id)
            .collect();
        recent_accesses.reverse();

        // Extract patterns of different lengths
        for pattern_len in self.min_pattern_length..=self.max_pattern_length {
            if recent_accesses.len() >= pattern_len {
                for i in 0..=(recent_accesses.len() - pattern_len) {
                    let pattern: Vec<PageId> = recent_accesses[i..i + pattern_len].to_vec();

                    // Update pattern weight using exponential moving average
                    let current_weight = self.pattern_weights.get(&pattern).unwrap_or(&0.0);
                    let new_weight = current_weight * (1.0 - self.learning_rate) + self.learning_rate;
                    self.pattern_weights.insert(pattern, new_weight);
                }
            }
        }

        // Decay old patterns
        for weight in self.pattern_weights.values_mut() {
            *weight *= 0.99; // Gradual decay
        }

        // Remove very low weight patterns
        self.pattern_weights.retain(|_, &mut weight| weight > 0.01);
    }

    /// Finds continuation of a pattern in history
    fn find_pattern_continuation(&self, pattern: &[PageId], offset: usize) -> Option<PageId> {
        let pattern_len = pattern.len();
        let access_vec: Vec<PageId> = self.access_history
            .iter()
            .map(|(page_id, _)| *page_id)
            .collect();

        for i in 0..=access_vec.len().saturating_sub(pattern_len + offset) {
            if &access_vec[i..i + pattern_len] == pattern
                && let Some(&next_page) = access_vec.get(i + pattern_len + offset - 1) {
                    return Some(next_page);
                }
        }

        None
    }

    /// Returns the current prediction accuracy
    pub fn get_accuracy(&self) -> f64 {
        self.prediction_accuracy
    }

    /// Stores last predictions to evaluate on subsequent accesses
    pub fn set_last_predictions(&mut self, predictions: Vec<PageId>) {
        self.last_predictions = Some(predictions);
    }
}

/// Prefetch engine that combines multiple prefetching strategies
pub struct PrefetchEngine {
    ml_prefetcher: MLPrefetcher,
    sequential_prefetch_enabled: bool,
    spatial_prefetch_enabled: bool,
    prefetch_distance: usize,
    prefetch_threshold: f64,
}

impl PrefetchEngine {
    pub fn new(prefetch_distance: usize) -> Self {
        Self {
            ml_prefetcher: MLPrefetcher::new(),
            sequential_prefetch_enabled: true,
            spatial_prefetch_enabled: true,
            prefetch_distance,
            prefetch_threshold: 0.5,
        }
    }

    pub fn record_access(&mut self, page_id: PageId) {
        self.ml_prefetcher.record_access(page_id);
    }

    pub fn predict_pages_to_prefetch(&self, current_page: PageId) -> Vec<PageId> {
        let mut predictions = Vec::new();
        
        // Get ML-based predictions
        if self.ml_prefetcher.get_accuracy() > self.prefetch_threshold {
            predictions.extend(self.ml_prefetcher.predict_prefetch(current_page));
        }
        
        // Add sequential prefetch if enabled
        if self.sequential_prefetch_enabled {
            for i in 1..=self.prefetch_distance {
                predictions.push(current_page + i as u64);
            }
        }
        
        // Remove duplicates and limit
        predictions.sort_unstable();
        predictions.dedup();
        predictions.truncate(16); // Reasonable limit
        
        predictions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ml_prefetcher_basic() {
        let mut prefetcher = MLPrefetcher::new();
        
        // Train with a simple pattern: 1,2,3,4,5
        for _ in 0..10 {
            for i in 1..=5 {
                prefetcher.record_access(i);
            }
        }
        
        // Test prediction after seeing page 3
        let predictions = prefetcher.predict_prefetch(3);
        
        // We expect at least page 4 to be predicted
        assert!(predictions.contains(&4));
    }

    #[test]
    fn test_prefetch_engine() {
        let mut engine = PrefetchEngine::new(4);
        
        // Record some accesses
        for i in 1..=10 {
            engine.record_access(i);
        }
        
        // Test predictions
        let predictions = engine.predict_pages_to_prefetch(10);
        
        // Sequential prefetching should predict at least 11
        assert!(predictions.contains(&11));
    }

    #[test]
    fn test_ml_prefetcher_no_predictions_with_short_history() {
        let mut prefetcher = MLPrefetcher::new();

        // With less than min_pattern_length accesses, there should be no patterns
        prefetcher.record_access(42);
        let predictions = prefetcher.predict_prefetch(42);
        assert!(predictions.is_empty());
    }

    #[test]
    fn test_ml_prefetcher_decay_reduces_old_patterns() {
        let mut prefetcher = MLPrefetcher::new();

        // Train with a predictable pattern to establish weights
        for _ in 0..20 {
            for i in 1..=5 {
                prefetcher.record_access(i);
            }
        }

        // Verify we initially get a sensible prediction for page 3
        let initial_predictions = prefetcher.predict_prefetch(3);
        assert!(initial_predictions.contains(&4));

        // Now add many unrelated accesses to trigger decay and pattern eviction
        // Use values far away from the trained pattern so they don't reinforce it
        for x in 1000u64..=2600u64 {
            prefetcher.record_access(x);
        }

        // Old patterns should have decayed below threshold and been pruned
        let decayed_predictions = prefetcher.predict_prefetch(3);
        assert!(decayed_predictions.is_empty());
    }

    #[test]
    fn test_prefetch_engine_sequential_only_predictions_respect_distance() {
        let engine = PrefetchEngine::new(3);
        let predictions = engine.predict_pages_to_prefetch(100);
        assert_eq!(predictions, vec![101, 102, 103]);
    }

    #[test]
    fn test_prefetch_engine_predictions_truncated_to_reasonable_limit() {
        let engine = PrefetchEngine::new(100);
        let predictions = engine.predict_pages_to_prefetch(1);
        // Truncated to 16 even if prefetch_distance is very large
        assert_eq!(predictions.len(), 16);
        // Should be the first 16 sequential pages starting after current page
        let expected: Vec<u64> = (2u64..=17u64).collect();
        assert_eq!(predictions, expected);
    }
}