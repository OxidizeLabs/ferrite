use std::sync::Once;

static INIT: Once = Once::new();

#[ctor::ctor]
fn setup() {
    initialize_logger();
}

pub fn initialize_logger() {
    INIT.call_once(|| {
        env_logger::init_from_env(env_logger::Env::default().default_filter_or("debug"));
    });
}

