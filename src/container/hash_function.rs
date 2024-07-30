use std::hash::Hasher;
use std::marker::PhantomData;
use xxhash_rust::xxh3;

/// Represents a hash function for a given key type.
pub struct HashFunction<K: ?Sized> {
    _marker: PhantomData<K>,
}

impl<K: ?Sized> HashFunction<K> {
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
    pub fn get_hash(&self, key: &K) -> u64
    where
        K: AsRef<[u8]>,
    {
        let mut hasher = xxh3::Xxh3::new();
        hasher.write(key.as_ref());
        hasher.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::HashFunction;

    #[test]
    fn test_hash_function() {
        let hash_function = HashFunction::<[u8]>::new();
        let key = b"test_key";
        let hash = hash_function.get_hash(key);
        assert_ne!(hash, 0);
    }
}
