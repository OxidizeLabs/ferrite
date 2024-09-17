use crate::common::rid::RID;
use crate::storage::table::table_heap::TableHeap;
use crate::storage::table::tuple::{Tuple, TupleMeta};
use parking_lot::Mutex;
use std::sync::Arc;
use futures::stream::iter;
use log::{error, info};
use crate::common::config::INVALID_PAGE_ID;
use crate::storage::page::page::PageTrait;
use crate::storage::page::page_types::table_page::TablePage;

pub struct TableIterator {
    table_heap: Arc<Mutex<TableHeap>>,
    rid: RID,
    stop_at_rid: RID,
}

impl TableIterator {
    pub fn new(table_heap: Arc<Mutex<TableHeap>>, rid: RID, stop_at_rid: RID) -> Self {
        if rid.get_page_id() == INVALID_PAGE_ID {
            let _rid = RID::new(INVALID_PAGE_ID, 0);
        } else {
            let table_page_lock = table_heap.lock();
            let bpm = table_page_lock.get_bpm();
            let page_guard = bpm.fetch_page_guarded(rid.get_page_id());
            match page_guard.unwrap().into_specific_type::<TablePage, 8>() {
                Some(mut ext_guard) => {
                    info!("Successfully converted to TablePage");

                    ext_guard.access(|page| {
                        info!("TablePage ID: {}", page.get_page_id());
                        if rid.get_slot_num() >= page.get_num_tuples() {
                            let _rid = RID::new(INVALID_PAGE_ID, 0);
                        }
                    });

                    ext_guard.access_mut(|page| {
                        page
                    });
                }
                None => {
                    error!("Failed to convert to TablePage");
                    panic!("Conversion to TablePage failed");
                }
            }
        }
        Self {
            table_heap,
            rid,
            stop_at_rid,
        }
    }

    pub fn get_tuple(&self) -> (TupleMeta, Tuple) {
        self.table_heap.lock().get_tuple(self.rid)
    }

    pub fn get_rid(&self) -> RID {
        self.rid
    }

    pub fn is_end(&self) -> bool {
        self.stop_at_rid == self.rid
    }
}

impl Iterator for TableIterator {
    type Item = (TupleMeta, Tuple);



    fn next(&mut self) -> Option<(TupleMeta, Tuple)> {
        for tuple in self.table_heap..iter() {}
    }
}