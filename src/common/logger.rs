use env_logger::Builder;
use log::LevelFilter;
use std::env;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn initialize_logger() {
    INIT.call_once(|| {
        let mut builder = Builder::new();

        builder
            .filter_level(LevelFilter::Debug)
            .filter_module("tkdb", LevelFilter::Debug)
            .filter_module("tkdb::buffer", LevelFilter::Debug)
            .filter_module("tkdb::storage", LevelFilter::Debug)
            .filter_module("tkdb::concurrency", LevelFilter::Debug)
            .filter_module("tkdb::recovery", LevelFilter::Debug)
            .filter_module("rustyline", LevelFilter::Info)
            .format_timestamp_millis()
            .parse_default_env()
            .init();
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use log::{debug, info};

    #[test]
    fn test_logging_levels() {
        env::set_var("RUST_TEST", "1");
        initialize_logger();
        debug!("Debug message in test");
        info!("Info message in test");
    }
}
