use std::time::{SystemTime, UNIX_EPOCH};

pub trait TimeSource: Send + Sync {
    fn now(&self) -> u64;
}

pub struct SystemTimeSource;

impl TimeSource for SystemTimeSource {
    fn now(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}
