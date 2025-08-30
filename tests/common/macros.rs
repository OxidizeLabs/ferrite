#[macro_export]
macro_rules! assert_ok {
    ($expr:expr) => {
        match $expr {
            Ok(val) => val,
            Err(err) => panic!("Expected Ok(_), got Err({:?})", err),
        }
    };
    ($expr:expr, $($arg:tt)+) => {
        match $expr {
            Ok(val) => val,
            Err(err) => panic!(concat!("Expected Ok(_): ", $($arg)+, ": {:?}"), err),
        }
    };
}

#[macro_export]
macro_rules! assert_err {
    ($expr:expr) => {
        if $expr.is_ok() { panic!("Expected Err(_), got Ok(_)" ); }
    };
    ($expr:expr, $($arg:tt)+) => {
        if $expr.is_ok() { panic!(concat!("Expected Err(_): ", $($arg)+)); }
    };
}


