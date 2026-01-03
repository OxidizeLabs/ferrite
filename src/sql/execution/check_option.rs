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

/// Individual optimizer transformation flags.
///
/// Each variant represents a specific optimization pass that can be selectively
/// enabled or disabled during query planning. The optimizer checks these flags
/// to determine which transformations to apply to the logical plan.
///
/// # Discriminant Values
///
/// The enum variants have explicit discriminant values for potential serialization
/// or debugging purposes:
/// - `EnableNljCheck` = 0
/// - `EnableTopnCheck` = 1
/// - `EnablePushdownCheck` = 2 (implicit)
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CheckOption {
    /// Enables transformation of Nested Loop Joins to Index Joins.
    ///
    /// When enabled, the optimizer will attempt to convert expensive nested
    /// loop joins into more efficient index-based joins when an appropriate
    /// index exists on the join key.
    EnableNljCheck  = 0,

    /// Enables merging of Sort + Limit operations into TopN.
    ///
    /// When enabled, the optimizer combines a Sort operator followed by a
    /// Limit operator into a single TopN operator, which is more efficient
    /// as it only maintains the top N elements during sorting.
    EnableTopnCheck = 1,

    /// Enables predicate pushdown optimization.
    ///
    /// When enabled, the optimizer pushes filter predicates as close to the
    /// data source as possible, reducing the amount of data processed by
    /// upstream operators.
    EnablePushdownCheck,
}

/// Set-based collection for managing optimizer transformation flags.
///
/// `CheckOptions` provides a convenient interface for enabling, disabling,
/// and querying optimizer flags. It uses a `HashSet` internally for O(1)
/// lookup performance.
///
/// # Default Behavior
///
/// A default `CheckOptions` instance has no optimizations enabled.
/// Use [`add_check()`](Self::add_check) to enable specific optimizations.
///
/// # Example
///
/// ```rust,ignore
/// let mut options = CheckOptions::new();
///
/// // Enable specific optimizations
/// options.add_check(CheckOption::EnableNljCheck);
/// options.add_check(CheckOption::EnableTopnCheck);
///
/// // Check if optimization is enabled
/// if options.has_check(&CheckOption::EnableNljCheck) {
///     // Apply NLJ to index join transformation
/// }
///
/// // Disable an optimization
/// options.remove_check(&CheckOption::EnableTopnCheck);
/// ```
#[derive(Default)]
pub struct CheckOptions {
    /// Set of currently enabled optimization flags.
    check_options_set: HashSet<CheckOption>,
}

impl CheckOptions {
    /// Creates a new `CheckOptions` with no optimizations enabled.
    ///
    /// Equivalent to [`CheckOptions::default()`](Self::default).
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let options = CheckOptions::new();
    /// assert!(options.is_empty());
    /// ```
    pub fn new() -> Self {
        Self {
            check_options_set: HashSet::new(),
        }
    }

    /// Enables an optimization flag.
    ///
    /// If the flag is already enabled, this is a no-op (idempotent).
    ///
    /// # Arguments
    ///
    /// * `option` - The optimization flag to enable
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut options = CheckOptions::new();
    /// options.add_check(CheckOption::EnableNljCheck);
    /// assert!(options.has_check(&CheckOption::EnableNljCheck));
    /// ```
    pub fn add_check(&mut self, option: CheckOption) {
        self.check_options_set.insert(option);
    }

    /// Disables an optimization flag.
    ///
    /// If the flag is not currently enabled, this is a no-op.
    ///
    /// # Arguments
    ///
    /// * `option` - Reference to the optimization flag to disable
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut options = CheckOptions::new();
    /// options.add_check(CheckOption::EnableNljCheck);
    /// options.remove_check(&CheckOption::EnableNljCheck);
    /// assert!(!options.has_check(&CheckOption::EnableNljCheck));
    /// ```
    pub fn remove_check(&mut self, option: &CheckOption) {
        self.check_options_set.remove(option);
    }

    /// Checks if an optimization flag is currently enabled.
    ///
    /// # Arguments
    ///
    /// * `option` - Reference to the optimization flag to check
    ///
    /// # Returns
    ///
    /// `true` if the optimization is enabled, `false` otherwise.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut options = CheckOptions::new();
    /// options.add_check(CheckOption::EnableTopnCheck);
    /// assert!(options.has_check(&CheckOption::EnableTopnCheck));
    /// assert!(!options.has_check(&CheckOption::EnableNljCheck));
    /// ```
    pub fn has_check(&self, option: &CheckOption) -> bool {
        self.check_options_set.contains(option)
    }

    /// Returns `true` if no optimizations are enabled.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let options = CheckOptions::new();
    /// assert!(options.is_empty());
    /// ```
    pub fn is_empty(&self) -> bool {
        self.check_options_set.is_empty()
    }

    /// Disables all optimization flags.
    ///
    /// After calling this method, [`is_empty()`](Self::is_empty) will return `true`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut options = CheckOptions::new();
    /// options.add_check(CheckOption::EnableNljCheck);
    /// options.add_check(CheckOption::EnableTopnCheck);
    /// options.clear();
    /// assert!(options.is_empty());
    /// ```
    pub fn clear(&mut self) {
        self.check_options_set.clear();
    }

    /// Returns `true` if any optimizations are enabled.
    ///
    /// This is the logical inverse of [`is_empty()`](Self::is_empty), indicating
    /// that the optimizer should apply some transformations to the query plan.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut options = CheckOptions::new();
    /// assert!(!options.is_modify());
    /// options.add_check(CheckOption::EnableNljCheck);
    /// assert!(options.is_modify());
    /// ```
    pub fn is_modify(&self) -> bool {
        !self.check_options_set.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;

    use parking_lot::RwLock;

    use super::*;

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
