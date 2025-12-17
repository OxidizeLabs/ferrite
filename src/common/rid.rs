use crate::common::config::{INVALID_PAGE_ID, PageId};
use bincode::{Decode, Encode};
use std::fmt;
use std::hash::Hash;

/// Represents a Record ID (RID) in the table.
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Hash, Ord, Encode, Decode)]
pub struct RID {
    page_id: PageId,
    slot_num: u32,
}

impl RID {
    /// Fixed-width on-disk/in-page encoding length (little-endian):
    /// `[page_id: u64][slot_num: u32]`.
    pub const ENCODED_LEN: usize = 8 + 4;

    /// Creates a new RID with the given page ID and slot number.
    ///
    /// # Arguments
    ///
    /// * `page_id` - The page identifier.
    /// * `slot_num` - The slot number within the page.
    pub const fn new(page_id: PageId, slot_num: u32) -> Self {
        Self { page_id, slot_num }
    }

    /// Creates an RID from a 64-bit integer representation.
    ///
    /// # Format
    ///
    /// This uses 32/32 packing:
    /// - high 32 bits: `page_id` (truncated to 32-bit)
    /// - low  32 bits: `slot_num`
    ///
    /// Prefer using `try_deserialize`/`to_bytes_le` for a lossless encoding.
    ///
    /// # Arguments
    ///
    /// * `rid` - The 64-bit integer representation of the RID.
    pub fn from_i64(rid: i64) -> Self {
        let bits = rid as u64;
        let page_id_u32 = (bits >> 32) as u32;
        let slot_num = bits as u32;
        Self::new(page_id_u32 as PageId, slot_num)
    }

    /// Returns the 64-bit integer representation of the RID.
    ///
    /// # Format and overflow handling
    ///
    /// This uses 32/32 packing (high 32 bits: `page_id`, low 32 bits: `slot_num`).
    /// If `page_id` does not fit in 32 bits, returns `None` instead of panicking.
    pub fn to_i64(&self) -> Option<i64> {
        if self.page_id > u32::MAX as PageId {
            return None;
        }
        let packed = ((self.page_id as u64) << 32) | (self.slot_num as u64);
        Some(packed as i64)
    }

    /// Returns the page ID of the RID.
    pub const fn get_page_id(&self) -> PageId {
        self.page_id
    }

    /// Returns the slot number of the RID.
    pub const fn get_slot_num(&self) -> u32 {
        self.slot_num
    }

    /// Serialize the RID as a fixed-width, little-endian byte array:
    /// `[page_id: u64][slot_num: u32]`.
    pub fn to_bytes_le(&self) -> [u8; Self::ENCODED_LEN] {
        let mut out = [0u8; Self::ENCODED_LEN];
        out[..8].copy_from_slice(&self.page_id.to_le_bytes());
        out[8..12].copy_from_slice(&self.slot_num.to_le_bytes());
        out
    }

    /// Try to deserialize a RID from `[page_id: u64][slot_num: u32]` (little-endian).
    pub fn try_deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < Self::ENCODED_LEN {
            return None;
        }
        let page_id = PageId::from_le_bytes(data[..8].try_into().ok()?);
        let slot_num = u32::from_le_bytes(data[8..12].try_into().ok()?);
        Some(Self::new(page_id, slot_num))
    }

    /// Deserialize a RID from `[page_id: u64][slot_num: u32]` (little-endian).
    ///
    /// # Panics
    ///
    /// Panics if `data` is shorter than `RID::ENCODED_LEN`.
    pub fn deserialize(data: &[u8]) -> Self {
        Self::try_deserialize(data).expect("RID::deserialize: buffer too small")
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
        assert_eq!(rid.to_i64(), Some(0x0000000100000002));
    }

    #[test]
    fn test_to_i64_overflow_returns_none() {
        let rid = RID::new((u32::MAX as PageId) + 1, 2);
        assert!(rid.to_i64().is_none());
    }

    #[test]
    fn test_serialize_deserialize_le() {
        let rid = RID::new(0x1122_3344_5566_7788, 0x99AA_BBCC);
        let bytes = rid.to_bytes_le();
        assert_eq!(bytes.len(), RID::ENCODED_LEN);
        let decoded = RID::deserialize(&bytes);
        assert_eq!(decoded, rid);
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
