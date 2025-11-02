#![allow(
    clippy::await_holding_lock,
    clippy::too_many_arguments,
    clippy::should_implement_trait,
    clippy::large_enum_variant,
    clippy::type_complexity,
    clippy::only_used_in_recursion,
    clippy::inherent_to_string,
    clippy::field_reassign_with_default,
    clippy::manual_div_ceil,
    clippy::unnecessary_mut_passed,
    clippy::unnecessary_unwrap,
    clippy::let_and_return,
    clippy::get_first,
    clippy::unnecessary_lazy_evaluations,
    clippy::needless_range_loop,
    clippy::manual_unwrap_or
)]
#![allow(dead_code, unused_mut)]

pub mod buffer;
pub mod catalog;
pub mod cli;
pub mod client;
pub mod common;
pub mod concurrency;
pub mod container;
pub mod network;
pub mod recovery;
pub mod server;
pub mod sql;
pub mod storage;
pub mod types_db;
