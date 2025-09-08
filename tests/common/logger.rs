use std::sync::Once;
use tkdb::common::logger as core_logger;

static INIT: Once = Once::new();

pub fn init_test_logger() {
    INIT.call_once(|| {
        // Prefer INFO level for CI noise; override via RUST_LOG when needed
        if std::env::var("RUST_LOG").is_err() {
            // set_var is safe in tests during init once
            unsafe {
                std::env::set_var("RUST_LOG", "info");
            }
        }
        core_logger::initialize_logger();
    });
}
