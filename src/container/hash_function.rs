use std::any::Any;
use std::hash::{Hash, Hasher};
use std::marker::PhantomData;
use xxhash_rust::xxh3;

/// Represents a hash function for a given key type.
pub struct HashFunction<K> {
    _marker: PhantomData<K>,
}

impl<K> HashFunction<K> {
    /// Creates a new `HashFunction`.
    ///
    /// # Returns
    /// A new `HashFunction` instance.
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

impl<K> HashFunction<K>
where
    K: Any + Hash + 'static,
{
    /// Returns the hash value of the given key.
    ///
    /// # Parameters
    /// - `key`: The key to be hashed.
    ///
    /// # Returns
    /// The hashed value.
    pub fn get_hash(&self, key: &K) -> u64 {
        let mut hasher = xxh3::Xxh3::new();

        match key as &dyn Any {
            key if key.is::<i32>() => hasher.write_i32(*key.downcast_ref::<i32>().unwrap()),
            key if key.is::<u32>() => hasher.write_u32(*key.downcast_ref::<u32>().unwrap()),
            key if key.is::<String>() => hasher.write(key.downcast_ref::<String>().unwrap().as_bytes()),
            key if key.is::<&str>() => hasher.write(key.downcast_ref::<&str>().unwrap().as_bytes()),
            _ => {
                // Fallback for types that implement `Hash`
                key.hash(&mut hasher);
            }
        }

        hasher.finish()
    }
}

// fn main() {
//     let hash_fn = HashFunction::new();
//
//     let key_i32 = 42;
//     let key_str = "hello";
//     let key_string = string::from("hello");
//
//     let hash_i32 = hash_fn.get_hash(&key_i32);
//     let hash_str = hash_fn.get_hash(&key_str);
//     let hash_string = hash_fn.get_hash(&key_string);
//
//     println!("Hash for i32: {}", hash_i32);
//     println!("Hash for &str: {}", hash_str);
//     println!("Hash for string: {}", hash_string);
// }
