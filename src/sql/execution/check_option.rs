//! # Optimizer Check Options
//!
//! This module provides a flag-based system for controlling which optimizer
//! transformations are applied during query planning. The optimizer uses these
//! flags to selectively enable or disable specific optimization passes.
//!
//! ## Architecture
//!
//! ```text
//!                      ┌─────────────────────────────────────┐
//!                      │           CheckOptions              │
//!                      │  ┌─────────────────────────────┐    │
//!                      │  │   HashSet<CheckOption>      │    │
//!                      │  │                             │    │
//!                      │  │  ┌─────────────────────┐    │    │
//!                      │  │  │ EnableNljCheck      │────┼────┼──► NLJ → Index Join
//!                      │  │  ├─────────────────────┤    │    │
//!                      │  │  │ EnableTopnCheck     │────┼────┼──► Sort+Limit → TopN
//!                      │  │  ├─────────────────────┤    │    │
//!                      │  │  │ EnablePushdownCheck │────┼────┼──► Predicate Pushdown
//!                      │  │  └─────────────────────┘    │    │
//!                      │  └─────────────────────────────┘    │
//!                      └─────────────────────────────────────┘
//! ```
//!
//! ## Key Components
//!
//! | Component       | Description                                           |
//! |-----------------|-------------------------------------------------------|
//! | `CheckOption`   | Enum of individual optimizer transformation flags    |
//! | `CheckOptions`  | Set-based collection for managing multiple flags     |
//!
//! ## Available Check Options
//!
//! | Flag                  | Value | Optimization Enabled                     |
//! |-----------------------|-------|------------------------------------------|
//! | `EnableNljCheck`      | 0     | Convert Nested Loop Join to Index Join   |
//! | `EnableTopnCheck`     | 1     | Merge Sort + Limit into TopN executor    |
//! | `EnablePushdownCheck` | 2     | Push predicates closer to data sources   |
//!
//! ## Optimization Flow
//!
//! ```text
//! ┌──────────────┐    ┌───────────────────┐    ┌──────────────────┐
//! │ Logical Plan │───►│     Optimizer     │───►│ Optimized Plan   │
//! └──────────────┘    │                   │    └──────────────────┘
//!                     │  CheckOptions:    │
//!                     │  ┌─────────────┐  │
//!                     │  │ NljCheck ✓  │──┼──► Apply NLJ→IndexJoin
//!                     │  │ TopN ✓      │──┼──► Apply Sort+Limit→TopN
//!                     │  │ Pushdown ✗  │──┼──► Skip predicate pushdown
//!                     │  └─────────────┘  │
//!                     └───────────────────┘
//! ```
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::sql::execution::check_option::{CheckOption, CheckOptions};
//!
//! // Create options with specific optimizations enabled
//! let mut options = CheckOptions::new();
//! options.add_check(CheckOption::EnableNljCheck);
//! options.add_check(CheckOption::EnableTopnCheck);
//!
//! // Check if optimization should be applied
//! if options.has_check(&CheckOption::EnableNljCheck) {
//!     // Apply nested loop join to index join transformation
//! }
//!
//! // Disable an optimization
//! options.remove_check(&CheckOption::EnableTopnCheck);
//!
//! // Check if any optimizations are enabled
//! if options.is_modify() {
//!     // Run optimizer with enabled transformations
//! }
//! ```
//!
//! ## Thread Safety
//!
//! The structs themselves are not internally synchronized. For concurrent access,
//! wrap in `Arc<RwLock<CheckOptions>>` as shown in the tests:
//!
//! ```rust,ignore
//! let options = Arc::new(RwLock::new(CheckOptions::new()));
//! let options_clone = Arc::clone(&options);
//!
//! // In another thread
//! let mut guard = options_clone.write();
//! guard.add_check(CheckOption::EnableNljCheck);
//! ```
//!
//! ## Design Notes
//!
//! - Uses `HashSet` for O(1) lookup of enabled flags
//! - Derives `Clone`, `PartialEq`, `Eq`, `Hash` for enum to support set operations
//! - `Default` implementation creates an empty set (no optimizations enabled)
//! - `is_modify()` returns true if any optimizations are pending

use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CheckOption {
    EnableNljCheck = 0,
    EnableTopnCheck = 1,
    EnablePushdownCheck,
}

#[derive(Default)]
pub struct CheckOptions {
    check_options_set: HashSet<CheckOption>,
}

impl CheckOptions {
    pub fn new() -> Self {
        Self {
            check_options_set: HashSet::new(),
        }
    }

    pub fn add_check(&mut self, option: CheckOption) {
        self.check_options_set.insert(option);
    }

    pub fn remove_check(&mut self, option: &CheckOption) {
        self.check_options_set.remove(option);
    }

    pub fn has_check(&self, option: &CheckOption) -> bool {
        self.check_options_set.contains(option)
    }

    pub fn is_empty(&self) -> bool {
        self.check_options_set.is_empty()
    }

    pub fn clear(&mut self) {
        self.check_options_set.clear();
    }

    pub fn is_modify(&self) -> bool {
        !self.check_options_set.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_check_options_new() {
        let options = CheckOptions::new();
        assert!(options.is_empty());
        assert!(!options.is_modify());
        assert_eq!(options.check_options_set.len(), 0);
    }

    #[test]
    fn test_add_check() {
        let mut options = CheckOptions::new();

        // Add first check
        options.add_check(CheckOption::EnableNljCheck);
        assert!(options.has_check(&CheckOption::EnableNljCheck));
        assert!(!options.has_check(&CheckOption::EnableTopnCheck));
        assert!(options.is_modify());

        // Add second check
        options.add_check(CheckOption::EnableTopnCheck);
        assert!(options.has_check(&CheckOption::EnableNljCheck));
        assert!(options.has_check(&CheckOption::EnableTopnCheck));
    }

    #[test]
    fn test_remove_check() {
        let mut options = CheckOptions::new();

        // Add and then remove check
        options.add_check(CheckOption::EnableNljCheck);
        assert!(options.has_check(&CheckOption::EnableNljCheck));

        options.remove_check(&CheckOption::EnableNljCheck);
        assert!(!options.has_check(&CheckOption::EnableNljCheck));
        assert!(options.is_empty());

        // Remove non-existent check (should not panic)
        options.remove_check(&CheckOption::EnableTopnCheck);
    }

    #[test]
    fn test_clear_checks() {
        let mut options = CheckOptions::new();

        // Add multiple checks
        options.add_check(CheckOption::EnableNljCheck);
        options.add_check(CheckOption::EnableTopnCheck);
        assert_eq!(options.check_options_set.len(), 2);

        // Clear all checks
        options.clear();
        assert!(options.is_empty());
        assert!(!options.is_modify());
    }

    #[test]
    fn test_duplicate_checks() {
        let mut options = CheckOptions::new();

        // Add same check multiple times
        options.add_check(CheckOption::EnableNljCheck);
        options.add_check(CheckOption::EnableNljCheck);
        options.add_check(CheckOption::EnableNljCheck);

        assert_eq!(options.check_options_set.len(), 1);
        assert!(options.has_check(&CheckOption::EnableNljCheck));
    }

    #[test]
    fn test_modify_state() {
        let mut options = CheckOptions::new();
        assert!(!options.is_modify());

        // Add check
        options.add_check(CheckOption::EnableNljCheck);
        assert!(options.is_modify());

        // Remove check
        options.remove_check(&CheckOption::EnableNljCheck);
        assert!(!options.is_modify());

        // Clear empty set
        options.clear();
        assert!(!options.is_modify());
    }

    #[test]
    fn test_check_option_values() {
        assert_eq!(CheckOption::EnableNljCheck as i32, 0);
        assert_eq!(CheckOption::EnableTopnCheck as i32, 1);
    }

    #[test]
    fn test_default_implementation() {
        let options = CheckOptions::default();
        assert!(options.is_empty());
        assert!(!options.is_modify());
    }

    #[test]
    fn test_all_operations_sequence() {
        let mut options = CheckOptions::new();

        // Test sequence of operations
        assert!(options.is_empty());

        options.add_check(CheckOption::EnableNljCheck);
        assert!(!options.is_empty());
        assert!(options.has_check(&CheckOption::EnableNljCheck));

        options.add_check(CheckOption::EnableTopnCheck);
        assert_eq!(options.check_options_set.len(), 2);

        options.remove_check(&CheckOption::EnableNljCheck);
        assert_eq!(options.check_options_set.len(), 1);
        assert!(options.has_check(&CheckOption::EnableTopnCheck));

        options.clear();
        assert!(options.is_empty());
    }

    #[test]
    fn test_concurrent_access() {
        let options = Arc::new(RwLock::new(CheckOptions::new()));
        let mut handles = vec![];

        // Spawn multiple threads to add and remove checks
        for i in 0..10 {
            let options_clone = Arc::clone(&options);
            let handle = thread::spawn(move || {
                let mut ops = options_clone.write();
                if i % 2 == 0 {
                    ops.add_check(CheckOption::EnableNljCheck);
                    ops.add_check(CheckOption::EnableTopnCheck);
                } else {
                    ops.remove_check(&CheckOption::EnableNljCheck);
                    ops.remove_check(&CheckOption::EnableTopnCheck);
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Final state should be consistent
        let final_options = options.read();
        assert!(final_options.check_options_set.len() <= 2);
    }

    #[test]
    fn test_check_option_clone() {
        let option = CheckOption::EnableNljCheck;
        let cloned = option.clone();
        assert_eq!(option, cloned);

        let mut options = CheckOptions::new();
        options.add_check(option);
        options.add_check(cloned);
        assert_eq!(options.check_options_set.len(), 1);
    }

    #[test]
    fn test_empty_operations() {
        let mut options = CheckOptions::new();

        // Operations on empty set
        assert!(options.is_empty());
        options.remove_check(&CheckOption::EnableNljCheck);
        assert!(options.is_empty());
        options.clear();
        assert!(options.is_empty());
        assert!(!options.has_check(&CheckOption::EnableNljCheck));
    }
}
