use std::sync::Once;

use env_logger::Builder;
use log::LevelFilter;

static INIT: Once = Once::new();

pub fn initialize_logger() {
    // Use call_once_force to recover if an earlier initialization attempt panicked.
    INIT.call_once_force(|_| {
        let mut builder = Builder::new();

        builder
            .filter_level(LevelFilter::Info)
            .filter_module("ferrite", LevelFilter::Info)
            .filter_module("ferrite::buffer", LevelFilter::Info)
            .filter_module("ferrite::storage", LevelFilter::Info)
            .filter_module("ferrite::concurrency", LevelFilter::Info)
            .filter_module("ferrite::recovery", LevelFilter::Info)
            .filter_module("rustyline", LevelFilter::Info)
            .format_timestamp_millis()
            .parse_default_env();

        // Avoid panicking if the logger was already initialized elsewhere.
        let _ = builder.try_init();
    });
}

#[cfg(test)]
mod tests {
    use log::{debug, info};

    use super::*;

    #[test]
    fn test_logging_levels() {
        initialize_logger();
        debug!("Debug message in test");
        info!("Info message in test");
    }
}
