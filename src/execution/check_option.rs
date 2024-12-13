use std::collections::HashSet;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CheckOption {
    EnableNljCheck = 0,
    EnableTopnCheck = 1,
}

#[derive(Default)]
pub struct CheckOptions {
    check_options_set: HashSet<CheckOption>,
}

impl CheckOptions {
    pub fn new() -> Self {
        Self {
            check_options_set: HashSet::new()
        }
    }

    pub fn add_check(&mut self, option: CheckOption) {
        self.check_options_set.insert(option);
    }

    pub fn remove_check(&mut self, option: &CheckOption) {
        self.check_options_set.remove(option);
    }

    pub fn has_check(&self, option: &CheckOption) -> bool {
        self.check_options_set.contains(option)
    }

    pub fn is_empty(&self) -> bool {
        self.check_options_set.is_empty()
    }

    pub fn clear(&mut self) {
        self.check_options_set.clear();
    }

    pub fn is_modify(&self) -> bool {
        !self.check_options_set.is_empty()
    }
}