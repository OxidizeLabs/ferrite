use std::sync::{Arc, Mutex};
use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
use tkdb::buffer::lru_k_replacer::LRUKReplacer;
use tkdb::catalogue::column::Column;
use tkdb::catalogue::schema::Schema;
use tkdb::storage::disk::disk_manager::DiskManager;
use tkdb::storage::disk::disk_scheduler::DiskScheduler;
use tkdb::storage::table::tuple::Tuple;
use tkdb::types_db::boolean_type::BooleanType;
use tkdb::types_db::integer_type::IntegerType;
use tkdb::types_db::type_id::TypeId;
use tkdb::types_db::types::Type;
use tkdb::types_db::value::Value;

fn main() {
    let boolean_type = BooleanType::new();
    let val = Value::new(true, );
    println!("{:?}", val);

    let col1 = Column::new("id".to_string(), TypeId::Integer);
    let col2 = Column::new_varlen("name".to_string(), TypeId::VarChar, 100);
    let col3 = Column::replicate("id_copy".to_string(), &col1);

    println!("Column 1: {}", col1);
    println!("Column 2: {}", col2);
    println!("Column 3: {}", col3);

    let mut storage = [0u8; 1];
    boolean_type.serialize_to(&val, &mut storage);
    println!("Serialized boolean storage: {:?}", storage);

    let deserialized_val = boolean_type.deserialize_from(&mut storage);
    println!("Deserialized boolean value: {:?}", deserialized_val);

    let columns = vec![
        Column::new("boolean_col".to_string(), TypeId::Boolean),
        Column::new("integer_col".to_string(), TypeId::Integer),
    ];

    let schema = Schema::new(columns);
    println!("{}", schema.to_string(false));
    println!("{}", schema.to_string(true));

    let integer_type = IntegerType::new();
    let value = Value::new(123456, );
    let mut storage = [0u8; 4];
    integer_type.serialize_to(&value, &mut storage);
    println!("Serialized integer storage: {:?}", storage);

    let deserialized_val = integer_type.deserialize_from(&mut storage);
    println!("Deserialized integer value: {:?}", deserialized_val);

    let v1 = Value::new(true, );
    let v2 = Value::new(111, );
    let values = vec![v1, v2];

    let tuple_schema = schema.clone(); // Clone the schema before passing it to Tuple::new
    let tuple = Tuple::new(values, tuple_schema, 1); // Use the cloned schema
    println!("Tuple: {:?}", tuple.to_string(&schema));

    // Using the copy_schema function
    let copied_schema = Schema::copy_schema(&schema, &vec![0, 1]);
    println!("Copied Schema: {:?}", copied_schema);

    let disk_manager = Arc::new(DiskManager::new("db_file", "db.log"));
    let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
    let replacer = Arc::new(Mutex::new(LRUKReplacer::new(10, 10)));
    let buffer_pool_manager = BufferPoolManager::new(100, disk_scheduler, disk_manager, replacer.clone());
    println!(
        "Buffer Pool Size: {:?}",
        buffer_pool_manager.get_pool_size()
    );

    // Create a new page
    if let Some(new_page) = buffer_pool_manager.new_page() {
        println!("Created new page: {:?}", new_page.get_page_id());
    }

    println!("Buffer Pool Pages: {:?}", buffer_pool_manager.get_pages());

    // Create a new page
    if let Some(new_page_1) = buffer_pool_manager.new_page() {
        println!("Created another new page: {:?}", new_page_1.get_page_id());
    }

    println!("Buffer Pool Pages 2: {:?}", buffer_pool_manager.get_pages());
    println!(
        "Number of frames in replacer: {:?}",
        replacer.lock().unwrap().size()
    );
}
