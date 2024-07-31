use std::fmt;
use std::hash::{Hash, Hasher};

use crate::common::config::{INVALID_PAGE_ID, PageId};

#[derive(Debug, Clone, Copy, Eq)]
pub struct RID {
    page_id: PageId,
    slot_num: u32,
}

impl RID {
    /// The default constructor creates an invalid RID!
    pub fn new() -> Self {
        RID {
            page_id: INVALID_PAGE_ID,
            slot_num: 0,
        }
    }

    /// Creates a new Record Identifier for the given page identifier and slot number.
    ///
    /// ## Arguments:
    /// * `page_id`: page identifier
    /// * `slot_num`: slot number
    pub fn with_page_and_slot(page_id: PageId, slot_num: u32) -> Self {
        RID { page_id, slot_num }
    }

    pub fn from_i64(rid: i64) -> Self {
        RID {
            page_id: (rid >> 32) as PageId,
            slot_num: rid as u32,
        }
    }

    pub fn get(&self) -> i64 {
        ((self.page_id as i64) << 32) | self.slot_num as i64
    }

    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    pub fn get_slot_num(&self) -> u32 {
        self.slot_num
    }

    pub fn set(&mut self, page_id: PageId, slot_num: u32) {
        self.page_id = page_id;
        self.slot_num = slot_num;
    }

    pub fn to_string(&self) -> String {
        format!("page_id: {} slot_num: {}\n", self.page_id, self.slot_num)
    }
}

impl fmt::Display for RID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_string())
    }
}

impl PartialEq for RID {
    fn eq(&self, other: &Self) -> bool {
        self.page_id == other.page_id && self.slot_num == other.slot_num
    }
}

impl Hash for RID {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.get().hash(state);
    }
}

impl Default for RID {
    fn default() -> Self {
        RID::new()
    }
}
