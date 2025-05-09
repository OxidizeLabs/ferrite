use crate::storage::index::types::key_types::KeyType;
use std::cmp::Ordering;

/// Type for i32 comparators
pub type I32Comparator = fn(&i32, &i32) -> Ordering;

/// Standard comparator for i32 keys
pub fn i32_comparator(a: &i32, b: &i32) -> Ordering {
    a.cmp(b)
}

/// Trait for key comparators
pub trait KeyComparator<K: KeyType>: Fn(&K, &K) -> Ordering {
    // Additional functionality can be added here
}

// Implement KeyComparator for function types
impl<K> KeyComparator<K> for fn(&K, &K) -> Ordering
where
    K: KeyType,
{
    // Implementation of any additional methods would go here
}
