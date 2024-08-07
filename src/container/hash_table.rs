
pub trait HashTable<K, V> {
    fn find(key: K, value: V) -> bool;
    fn remove(key:K) -> bool;
    fn insert(key: K, value:V) -> ();
}
