use crate::common::config::{PageId, INVALID_PAGE_ID};
use std::fmt;
use std::hash::Hash;
use bincode::{Decode, Encode};

/// Represents a Record ID (RID) in the table.
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Hash, Ord, Encode, Decode)]
pub struct RID {
    page_id: PageId,
    slot_num: u32,
}

impl RID {
    /// Creates a new RID with the given page ID and slot number.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page identifier.
    /// * `slot_num` - The slot number within the page.
    pub fn new(page_id: PageId, slot_num: u32) -> Self {
        Self { page_id, slot_num }
    }

    /// Creates an RID from a 64-bit integer representation.
    ///
    /// # Arguments
    ///
    /// * `rid` - The 64-bit integer representation of the RID.
    pub fn from_i64(rid: i64) -> Self {
        Self {
            page_id: (rid >> 32) as PageId,
            slot_num: rid as u32,
        }
    }

    /// Returns the 64-bit integer representation of the RID.
    pub fn to_i64(&self) -> i64 {
        ((self.page_id as i64) << 32) | self.slot_num as i64
    }

    /// Returns the page ID of the RID.
    pub fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Returns the slot number of the RID.
    pub fn get_slot_num(&self) -> u32 {
        self.slot_num
    }

    pub fn deserialize(data: &[u8]) -> Self {
        let page_id = u32::from_le_bytes(data[..4].try_into().unwrap());
        let slot_num = u32::from_le_bytes(data[4..8].try_into().unwrap());
        RID::new(page_id as PageId, slot_num)
    }
}

impl fmt::Display for RID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "page_id: {} slot_num: {}", self.page_id, self.slot_num)
    }
}

impl Default for RID {
    fn default() -> Self {
        Self {
            page_id: INVALID_PAGE_ID,
            slot_num: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let rid = RID::new(1, 2);
        assert_eq!(rid.get_page_id(), 1);
        assert_eq!(rid.get_slot_num(), 2);
    }

    #[test]
    fn test_from_i64() {
        let rid = RID::from_i64(0x0000000100000002);
        assert_eq!(rid.get_page_id(), 1);
        assert_eq!(rid.get_slot_num(), 2);
    }

    #[test]
    fn test_to_i64() {
        let rid = RID::new(1, 2);
        assert_eq!(rid.to_i64(), 0x0000000100000002);
    }

    #[test]
    fn test_default() {
        let rid = RID::default();
        assert_eq!(rid.get_page_id(), INVALID_PAGE_ID);
        assert_eq!(rid.get_slot_num(), 0);
    }

    #[test]
    fn test_display() {
        let rid = RID::new(1, 2);
        assert_eq!(format!("{}", rid), "page_id: 1 slot_num: 2");
    }

    #[test]
    fn test_eq() {
        let rid1 = RID::new(1, 2);
        let rid2 = RID::new(1, 2);
        let rid3 = RID::new(1, 3);
        assert_eq!(rid1, rid2);
        assert_ne!(rid1, rid3);
    }

    #[test]
    fn test_hash() {
        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(RID::new(1, 2));
        assert!(set.contains(&RID::new(1, 2)));
        assert!(!set.contains(&RID::new(1, 3)));
    }
}
