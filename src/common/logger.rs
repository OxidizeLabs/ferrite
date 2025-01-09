use log::debug;
use env_logger::Builder;
use log::LevelFilter;
use std::env;
use std::sync::Once;
use std::io::Write;

static INIT: Once = Once::new();

pub fn initialize_logger() {
    INIT.call_once(|| {
        let mut builder = Builder::new();
        
        builder
            .filter_level(LevelFilter::Debug)  // Always set to Debug in tests
            .format(|buf, record| {
                writeln!(buf,
                    "{} [{}] {} - {}",
                    chrono::Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
                    record.level(),
                    record.target(),
                    record.args()
                )
            })
            .init();
        
        debug!("Logger initialized at Debug level");
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
