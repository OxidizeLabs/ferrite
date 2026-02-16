//! # Record ID (RID)
//!
//! This module provides the `RID` (Record ID) type, the fundamental identifier for locating
//! individual records (tuples) within a table. Every record stored in the database has a
//! unique RID that specifies its physical location.
//!
//! ## Architecture
//!
//! ```text
//!   ┌──────────────────────────────────────────────────────────────────────────┐
//!   │                              RID                                         │
//!   │                                                                          │
//!   │   ┌─────────────────────────────┐  ┌─────────────────────────────┐       │
//!   │   │        page_id: u64         │  │       slot_num: u32         │       │
//!   │   │                             │  │                             │       │
//!   │   │  Which page in the table    │  │  Which slot within the page │       │
//!   │   │  heap contains this record  │  │  (index into slot directory)│       │
//!   │   └─────────────────────────────┘  └─────────────────────────────┘       │
//!   │                                                                          │
//!   │   Total size: 12 bytes (8 + 4)                                           │
//!   └──────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Physical Location Mapping
//!
//! ```text
//!   RID { page_id: 42, slot_num: 3 }
//!                │              │
//!                │              │
//!                ▼              │
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  Table Heap (doubly linked list of pages)                              │
//!   │                                                                        │
//!   │   Page 40 ──► Page 41 ──► Page 42 ──► Page 43 ──► ...                  │
//!   │                              │                                         │
//!   └──────────────────────────────┼─────────────────────────────────────────┘
//!                                  │
//!                                  ▼
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  Page 42 (Slotted Page Format)                                         │
//!   │                                                                        │
//!   │  ┌──────────────────────────────────────────────────────────────────┐  │
//!   │  │ Header │ Slot 0 │ Slot 1 │ Slot 2 │ Slot 3 │ ... │ Free │ Tuples │  │
//!   │  └────────────────────────────────────┼────────────────────────────────┘  │
//!   │                                       │                                │
//!   │                                       ▼                                │
//!   │                               slot_num = 3                             │
//!   │                                       │                                │
//!   │                                       ▼                                │
//!   │                              ┌──────────────────┐                      │
//!   │                              │  Tuple Data      │                      │
//!   │                              │  (actual record) │                      │
//!   │                              └──────────────────┘                      │
//!   └────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Serialization Formats
//!
//! ```text
//!   Fixed-Width Little-Endian (12 bytes) - Preferred for storage
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   Byte:   0    1    2    3    4    5    6    7    8    9   10   11
//!         ┌────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┬────┐
//!         │         page_id (u64 LE)             │  slot_num (u32 LE) │
//!         └────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┴────┘
//!
//!   Methods: to_bytes_le() / try_deserialize() / deserialize()
//!
//!
//!   i64 Packed Format (8 bytes) - For compact in-memory representation
//!   ═══════════════════════════════════════════════════════════════════════════
//!
//!   ┌────────────────────────────────────────────────────────────────────────┐
//!   │  High 32 bits: page_id (truncated!)  │  Low 32 bits: slot_num         │
//!   └────────────────────────────────────────────────────────────────────────┘
//!
//!   ⚠️ Warning: page_id is truncated to 32 bits! Use only when page_id < 2^32.
//!   Methods: to_i64() → Option<i64> / from_i64(i64)
//! ```
//!
//! ## Key Components
//!
//! | Field       | Type     | Size    | Description                          |
//! |-------------|----------|---------|--------------------------------------|
//! | `page_id`   | `PageId` | 8 bytes | Page identifier in table heap       |
//! | `slot_num`  | `u32`    | 4 bytes | Slot index within the page           |
//!
//! ## Core Operations
//!
//! | Method              | Returns              | Description                      |
//! |---------------------|----------------------|----------------------------------|
//! | `new(page, slot)`   | `RID`                | Create new RID                   |
//! | `get_page_id()`     | `PageId`             | Get page identifier              |
//! | `get_slot_num()`    | `u32`                | Get slot number                  |
//! | `to_bytes_le()`     | `[u8; 12]`           | Serialize to bytes (lossless)    |
//! | `try_deserialize()` | `Option<RID>`        | Deserialize from bytes           |
//! | `deserialize()`     | `RID`                | Deserialize (panics if invalid)  |
//! | `to_i64()`          | `Option<i64>`        | Pack to i64 (may fail)           |
//! | `from_i64(i64)`     | `RID`                | Unpack from i64                  |
//!
//! ## Trait Implementations
//!
//! | Trait           | Purpose                                         |
//! |-----------------|-------------------------------------------------|
//! | `Clone`, `Copy` | RID is small, cheap to copy                     |
//! | `Eq`, `PartialEq`| Compare RIDs for equality                      |
//! | `Ord`, `PartialOrd`| Order RIDs (page_id first, then slot_num)   |
//! | `Hash`          | Use RIDs as HashMap/HashSet keys                |
//! | `Encode`,`Decode`| Bincode serialization for persistence          |
//! | `Display`       | Human-readable format                           |
//! | `Default`       | Invalid RID (INVALID_PAGE_ID, slot 0)           |
//!
//! ## Example Usage
//!
//! ```rust,ignore
//! use crate::common::rid::RID;
//!
//! // Create a RID
//! let rid = RID::new(42, 3);
//! assert_eq!(rid.get_page_id(), 42);
//! assert_eq!(rid.get_slot_num(), 3);
//!
//! // Serialize to bytes (lossless, 12 bytes)
//! let bytes = rid.to_bytes_le();
//! let restored = RID::deserialize(&bytes);
//! assert_eq!(rid, restored);
//!
//! // Pack to i64 (compact but lossy for large page_ids)
//! if let Some(packed) = rid.to_i64() {
//!     let unpacked = RID::from_i64(packed);
//!     assert_eq!(rid, unpacked);
//! }
//!
//! // Use in collections
//! use std::collections::HashMap;
//! let mut index: HashMap<RID, String> = HashMap::new();
//! index.insert(rid, "record data".to_string());
//!
//! // Display
//! println!("{}", rid); // Output: "page_id: 42 slot_num: 3"
//!
//! // Default (invalid RID)
//! let invalid = RID::default();
//! assert_eq!(invalid.get_page_id(), crate::common::config::INVALID_PAGE_ID);
//! ```
//!
//! ## Usage in Database Operations
//!
//! ```text
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ INSERT INTO users VALUES (1, 'Alice')                                   │
//!   │                                                                         │
//!   │   1. TableHeap allocates space on a page                                │
//!   │   2. Returns RID { page_id: 5, slot_num: 12 }                           │
//!   │   3. Index stores: key='Alice' → RID(5, 12)                             │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ SELECT * FROM users WHERE name = 'Alice'                                │
//!   │                                                                         │
//!   │   1. Index lookup: 'Alice' → RID(5, 12)                                 │
//!   │   2. TableHeap.get_tuple(RID(5, 12))                                    │
//!   │   3. BufferPool fetches page 5                                          │
//!   │   4. Page returns tuple at slot 12                                      │
//!   └─────────────────────────────────────────────────────────────────────────┘
//!
//!   ┌─────────────────────────────────────────────────────────────────────────┐
//!   │ UPDATE users SET age = 30 WHERE name = 'Alice'                          │
//!   │                                                                         │
//!   │   1. Index lookup: 'Alice' → RID(5, 12)                                 │
//!   │   2. MVCC: Mark old version, create new version                         │
//!   │   3. New RID (possibly same page) or version chain                      │
//!   └─────────────────────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Thread Safety
//!
//! - `RID` is `Copy` and immutable - inherently thread-safe
//! - Can be freely shared across threads without synchronization
//! - Commonly used as keys in concurrent data structures
//!
//! ## Implementation Notes
//!
//! - **ENCODED_LEN**: 12 bytes fixed-width for storage consistency
//! - **i64 Packing**: Truncates page_id to 32 bits - use `to_bytes_le()` for full fidelity
//! - **Ordering**: `Ord` compares page_id first, then slot_num (useful for sequential scans)
//! - **Default**: Returns invalid RID using `INVALID_PAGE_ID` sentinel value

use std::fmt;
use std::hash::Hash;

use serde::{Deserialize, Serialize};

use crate::common::config::{INVALID_PAGE_ID, PageId};

/// Record ID (RID) - identifies a record's physical location in a table.
///
/// A RID consists of a page ID and slot number, together uniquely identifying
/// the physical location of a tuple within a table heap.
///
/// See module-level documentation for details.
#[derive(Debug, Clone, Copy, Eq, PartialEq, PartialOrd, Hash, Ord, Serialize, Deserialize)]
pub struct RID {
    /// The page identifier within the table heap.
    page_id: PageId,
    /// The slot index within the page's slot directory.
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
        let packed = (self.page_id << 32) | (self.slot_num as u64);
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

/// Formats the RID as `"page_id: <id> slot_num: <num>"`.
impl fmt::Display for RID {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "page_id: {} slot_num: {}", self.page_id, self.slot_num)
    }
}

/// Returns an invalid RID using [`INVALID_PAGE_ID`] and slot 0.
///
/// This sentinel value indicates an uninitialized or deleted record.
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
