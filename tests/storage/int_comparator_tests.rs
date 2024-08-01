use tkdb::storage::index::int_comparator::GenericComparator;

#[cfg(test)]
mod tests {
    use super::GenericComparator;

    #[test]
    fn test_int_comparator() {
        let comparator = GenericComparator;

        assert_eq!(comparator.compare(1, 2), -1);
        assert_eq!(comparator.compare(2, 1), 1);
        assert_eq!(comparator.compare(3, 3), 0);
    }
}
