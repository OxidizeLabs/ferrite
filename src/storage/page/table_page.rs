use std::collections::HashMap;
use std::hash::{BuildHasherDefault, Hasher};
use std::option::Option;

use xxhash_rust::xxh3::Xxh3;

use crate::common::config::PageId;
use crate::storage::table::tuple::{Tuple, TupleMeta};

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct RID {}

// Create a type alias for our custom hasher
type XxHashBuilder = BuildHasherDefault<Xxh3Hasher>;

pub struct TablePage {
    page_start: Vec<u8>,
    next_page_id: PageId,
    num_tuples: u16,
    num_deleted_tuples: u16,
    tuple_info: HashMap<RID, (TupleMeta, Tuple), XxHashBuilder>,
}

impl TablePage {
    pub fn new(page_id: PageId) -> Self {
        let map = HashMap::with_hasher(BuildHasherDefault::<Xxh3Hasher>::default());
        Self {
            page_start: vec![],
            next_page_id: page_id,
            num_tuples: 0,
            num_deleted_tuples: 0,
            tuple_info: map,
        }
    }

    pub fn get_num_tuples(&self) -> u32 {
        self.num_tuples as u32
    }

    pub fn get_next_page_id(&self) -> PageId {
        self.next_page_id
    }

    pub fn set_next_page_id(&mut self, next_page_id: PageId) {
        self.next_page_id = next_page_id;
    }

    pub fn get_next_tuple_offset(&self, meta: &TupleMeta, tuple: &Tuple) -> Option<u16> {
        unimplemented!()
    }

    pub fn insert_tuple(&mut self, meta: &TupleMeta, tuple: &Tuple) -> Option<u16> {
        unimplemented!()
    }

    pub fn update_tuple_meta(&mut self, meta: &TupleMeta, rid: &RID) {
        unimplemented!()
    }

    pub fn get_tuple(&self, rid: &RID) -> Option<(&TupleMeta, &Tuple)> {
        match self.tuple_info.get(rid).unwrap() {
            (tuple_meta, tuple) => Some((tuple_meta, tuple))
        }
    }

    pub fn get_tuple_meta(&self, rid: &RID) -> Option<&TupleMeta> {
        match self.tuple_info.get(rid).unwrap() {
            (tuple_meta, _tuple) => Some(tuple_meta),
            _ => None,
        }
    }

    pub fn update_tuple_in_place_unsafe(&mut self, meta: &TupleMeta, tuple: &Tuple, rid: &RID) {
        unimplemented!()
    }
}

// Custom hasher struct to wrap Xxh3 hasher
#[derive(Default)]
struct Xxh3Hasher {
    hasher: Xxh3,
}

impl Hasher for Xxh3Hasher {
    fn finish(&self) -> u64 {
        self.hasher.clone().digest()
    }

    fn write(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }
}

// fn main() {
//     // Example usage of TablePage
//     let mut table_page = TablePage::new(PageId::new());
//     table_page.init();
//
//     // Further code...
// }
