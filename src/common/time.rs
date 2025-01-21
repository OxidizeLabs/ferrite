use std::fmt::Display;
use std::time::{SystemTime, UNIX_EPOCH};
use serde::{Deserialize, Serialize};

pub trait TimeSource: Send + Sync {
    fn now(&self) -> u64;
}

pub struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct TimeStamp(u64);

impl TimeStamp {
    pub const MAX: TimeStamp = TimeStamp(u64::MAX);
    pub const MIN: TimeStamp = TimeStamp(0);

    pub fn new(ts: u64) -> Self {
        Self(ts)
    }

    pub fn get(&self) -> u64 {
        self.0
    }
}

impl Display for TimeStamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}
