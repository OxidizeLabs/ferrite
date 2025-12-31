// Clippy configuration - see individual items for rationale
//
// Legitimate suppressions with documented reasoning:
#![allow(
    // Required: Lock guards held across await points is intentional in our BPM design.
    // The async disk manager coordinates with held page locks by design.
    clippy::await_holding_lock
)]

pub mod buffer;
pub mod catalog;
pub mod cli;
pub mod client;
pub mod common;
pub mod concurrency;
pub mod container;
pub mod recovery;
pub mod server;
pub mod sql;
pub mod storage;
pub mod types_db;
