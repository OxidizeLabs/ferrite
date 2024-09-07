use crate::types_db::value::Value;
use std::any::Any;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use xxhash_rust::xxh3::Xxh3;

// Custom hasher struct to wrap Xxh3 hasher
#[derive(Default)]
pub struct Xxh3Hasher {
    hasher: Xxh3,
}

/// Represents a hash function for a given key type.
pub struct HashFunction<K> {
    _marker: PhantomData<K>,
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
            key if key.is::<String>() => hasher.write(key.downcast_ref::<String>().unwrap().as_bytes()),
            key if key.is::<&str>() => hasher.write(key.downcast_ref::<&str>().unwrap().as_bytes()),
            key if key.is::<&Value>() => hasher.write(&key.downcast_ref::<Value>().unwrap().as_bytes()),
            _ => {
                // Fallback for types that implement `Hash`
                key.hash(&mut hasher);
            }
        }

        hasher.finish()
    }
}

impl Hasher for Xxh3Hasher {
    fn finish(&self) -> u64 {
        self.hasher.clone().digest()
    }

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
