#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use rand::{Rng, SeedableRng};
    use rand::prelude::StdRng;
    use tkdb::buffer::buffer_pool_manager::BufferPoolManager;
    use tkdb::buffer::lru_k_replacer::LRUKReplacer;
    use tkdb::catalogue::column::Column;
    use tkdb::catalogue::schema::Schema;
    use tkdb::common::rwlatch::ReaderWriterLatch;
    use tkdb::storage::disk::disk_manager::DiskManager;
    use tkdb::storage::disk::disk_scheduler::DiskScheduler;
    use tkdb::storage::table::table_heap::TableHeap;
    use tkdb::storage::table::tuple::{Tuple, TupleMeta};
    use tkdb::types_db::type_id::TypeId;
    use tkdb::types_db::value::Value;

    extern crate tkdb;

    fn construct_tuple(schema: &Schema) -> Tuple {
        let mut values = Vec::new();
        let seed = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let mut rng = StdRng::seed_from_u64(seed);

        for i in 0..schema.get_column_count() {
            let col = schema.get_column(i as usize);
            let type_id = col.expect("REASON").get_type();
            let value = match type_id {
                TypeId::Boolean => Value::new(rng.gen::<bool>()),
                TypeId::TinyInt => Value::new(rng.gen::<i8>() % 100),
                TypeId::SmallInt => Value::new(rng.gen::<i16>() % 1000),
                TypeId::Integer => Value::new(rng.gen::<i32>() % 1000),
                TypeId::BigInt => Value::new(rng.gen::<i64>() % 1000),
                TypeId::VarChar => {
                    let alphanum = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
                    let len = 1 + rng.gen::<usize>() % 9;
                    let s: String = (0..len).map(|_| {
                        let idx = rng.gen::<usize>() % alphanum.len();
                        alphanum[idx] as char
                    }).collect();
                    Value::new(s)
                }
                _ => continue,
            };
            values.push(value);
        }
        Tuple::new(values, schema.clone(), 0)
    }

    #[test]
    fn disabled_table_heap_test() {
        // Test 1: Parse create SQL statement
        let create_stmt = "a varchar(20), b smallint, c bigint, d bool, e varchar(16)";
        let col1 = Column::new_varlen("a".parse().unwrap(), TypeId::VarChar, 4);
        let col2 = Column::new("b".parse().unwrap(), TypeId::SmallInt);
        let col3 = Column::new("c".parse().unwrap(), TypeId::BigInt);
        let col4 = Column::new("d".parse().unwrap(), TypeId::Boolean);
        let col5 = Column::new_varlen("e".parse().unwrap(), TypeId::VarChar, 8);
        let cols = vec![col1, col2, col3, col4, col5];
        let schema = Schema::new(cols);
        let tuple = construct_tuple(&schema);

        // Create transaction
        let disk_manager = Arc::new(DiskManager::new("tuple_test.db", "tuple_test.log"));
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(10, 10)));
        let buffer_pool_manager = BufferPoolManager::new(100, disk_scheduler, disk_manager, replacer.clone());

        let table_latch = ReaderWriterLatch::new();
        let table = TableHeap::new(Arc::new(buffer_pool_manager));

        let mut rid_v = vec![];
        for _ in 0..5000 {
            let rid = table.insert_tuple(&TupleMeta::new(0, false), &tuple, None, None, 0);
            rid_v.push(rid);
        }

        // let mut itr = table.make_iterator();
        // // Print the tuple string representation (if needed
        // itr.try_into().
        // println!("{:?}", itr.to_string());

        // Clean up
        // disk_manager.shutdown();
        fs::remove_file("tuple_test.db").unwrap();
        fs::remove_file("tuple_test.log").unwrap();
    }
}