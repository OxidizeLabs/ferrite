//! # Hash Function Utilities
//!
//! This module provides hash function implementations for use in hash-based
//! data structures like the extendable hash table. It uses the XXH3 algorithm
//! from the `xxhash-rust` crate for fast, high-quality hashing.
//!
//! ## Components
//!
//! - [`HashFunction`]: A generic hash function wrapper that handles common types.
//! - [`Xxh3Hasher`]: A [`Hasher`] implementation wrapping XXH3 for use with Rust's
//!   standard `Hash` trait.

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;

use xxhash_rust::xxh3::Xxh3;

/// A [`Hasher`] implementation wrapping the XXH3 algorithm.
///
/// This struct adapts the `Xxh3` hasher to implement Rust's standard [`Hasher`]
/// trait, enabling use with types that implement [`Hash`].
///
/// # Example
///
/// ```rust,ignore
/// use std::hash::{Hash, Hasher};
/// use crate::container::hash_function::Xxh3Hasher;
///
/// let mut hasher = Xxh3Hasher::default();
/// "hello".hash(&mut hasher);
/// let hash = hasher.finish();
/// ```
#[derive(Default)]
pub struct Xxh3Hasher {
    /// The underlying XXH3 hasher from the `xxhash-rust` crate.
    hasher: Xxh3,
}

/// A generic hash function for computing hash values from keys.
///
/// This struct provides optimized hashing for common types (`i32`, `u32`, `String`, `&str`)
/// with a fallback to the standard [`Hash`] trait for other types. It uses the XXH3
/// algorithm internally for fast, high-quality hash values.
///
/// # Type Parameters
///
/// - `K`: The key type to hash. Must implement `Any + Hash + 'static`.
///
/// # Example
///
/// ```rust,ignore
/// use crate::container::hash_function::HashFunction;
///
/// let hash_fn = HashFunction::<i32>::new();
/// let hash = hash_fn.get_hash(&42);
/// println!("Hash of 42: {}", hash);
/// ```
pub struct HashFunction<K> {
    /// Phantom data to carry the key type without storing it.
    _marker: PhantomData<K>,
}

impl<K> Default for HashFunction<K>
where
    K: Any + Hash + 'static,
{
    /// Returns a new `HashFunction` instance.
    ///
    /// Equivalent to calling [`HashFunction::new()`].
    fn default() -> Self {
        Self::new()
    }
}

impl<K> HashFunction<K>
where
    K: Any + Hash + 'static,
{
    /// Creates a new `HashFunction`.
    ///
    /// # Returns
    /// A new `HashFunction` instance.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }

    /// Returns the hash value of the given key.
    ///
    /// # Parameters
    /// - `key`: The key to be hashed.
    ///
    /// # Returns
    /// The hashed value.
    pub fn get_hash(&self, key: &K) -> u64 {
        let mut hasher = Xxh3::new();

        match key as &dyn Any {
            key if key.is::<i32>() => hasher.write_i32(*key.downcast_ref::<i32>().unwrap()),
            key if key.is::<u32>() => hasher.write_u32(*key.downcast_ref::<u32>().unwrap()),
            key if key.is::<String>() => {
                hasher.write(key.downcast_ref::<String>().unwrap().as_bytes())
            },
            key if key.is::<&str>() => hasher.write(key.downcast_ref::<&str>().unwrap().as_bytes()),
            _ => {
                // Fallback for types that implement `Hash`
                key.hash(&mut hasher);
            },
        }

        hasher.finish()
    }
}

impl Hasher for Xxh3Hasher {
    /// Finalizes the hash computation and returns the resulting hash value.
    ///
    /// Note: This clones the internal hasher to compute the digest, allowing
    /// the hasher to continue accumulating data after this call.
    ///
    /// # Returns
    ///
    /// The 64-bit XXH3 hash value.
    fn finish(&self) -> u64 {
        self.hasher.clone().digest()
    }

    /// Writes bytes to the hasher, updating its internal state.
    ///
    /// # Parameters
    ///
    /// - `bytes`: The byte slice to incorporate into the hash.
    fn write(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }
}

#[cfg(test)]
mod unit_tests {
    use super::HashFunction;
    use crate::types_db::value::Value;

    #[test]
    fn hash_function_with_i32() {
        let hash_function = HashFunction::<i32>::new();
        let key = 42;
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }

    #[test]
    fn hash_function_with_u32() {
        let hash_function = HashFunction::<u32>::new();
        let key = 42u32;
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }

    #[test]
    fn hash_function_with_str() {
        let hash_function = HashFunction::<&str>::new();
        let key = "test_key";
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }

    #[test]
    fn hash_function_with_string() {
        let hash_function = HashFunction::<String>::new();
        let key = String::from("test_key");
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }

    #[test]
    fn hash_function_with_value() {
        let hash_function = HashFunction::<Value>::new();
        let key = Value::from(42);
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }
}
