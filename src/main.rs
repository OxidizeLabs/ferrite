extern crate tkdb;

use tkdb::types::get_instance;
use tkdb::value::Value;
use tkdb::type_id::TypeId;

fn main() {

    let _boolean_type = get_instance(TypeId::Boolean);
    let val = Value::new(TypeId::Boolean);
    println!("{:?}", val);
}
