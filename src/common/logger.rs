use env_logger::Builder;
use log::LevelFilter;
use std::env;
use std::sync::Once;

static INIT: Once = Once::new();

pub fn initialize_logger() {
    INIT.call_once(|| {
        let mut builder = Builder::new();

        let is_test = env::var("RUST_TEST").is_ok();
        let default_level = if is_test {
            LevelFilter::Debug
        } else {
            LevelFilter::Info
        };

        builder
            .filter_level(default_level)
            .filter_module("rustyline", LevelFilter::Warn)
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
