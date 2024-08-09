use tkdb::container::hash_function::HashFunction;

#[cfg(test)]
mod tests {
    use super::HashFunction;

    #[test]
    fn test_hash_function_with_i32() {
        let hash_function = HashFunction::<i32>::new();
        let key = 42;
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_function_with_u32() {
        let hash_function = HashFunction::<u32>::new();
        let key = 42u32;
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_function_with_str() {
        let hash_function = HashFunction::<&str>::new();
        let key = "test_key";
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }

    #[test]
    fn test_hash_function_with_string() {
        let hash_function = HashFunction::<String>::new();
        let key = String::from("test_key");
        let hash = hash_function.get_hash(&key);
        assert_ne!(hash, 0);
    }
}
