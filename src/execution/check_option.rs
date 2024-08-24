use std::collections::HashSet;

pub enum CheckOption {
    EnableNljCheck = 0,
    EnableTopnCheck = 1,
}

#[derive(Default)]
pub struct CheckOptions {
    _check_options_set: HashSet<CheckOption>,
}
