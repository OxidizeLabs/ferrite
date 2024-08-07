use tkdb::container::hash_function::HashFunction;

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
