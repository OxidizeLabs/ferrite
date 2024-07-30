#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};
    use rand::prelude::StdRng;
    use rand::{Rng, SeedableRng};
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::common::rwlatch::ReaderWriterLatch;
    use crate::disk::disk_manager::DiskManager;
    use crate::disk::disk_scheduler::DiskScheduler;
    use crate::table::tuple::{Tuple, TupleMeta};
    use crate::types_db::type_id::TypeId;
    use crate::table::table_heap::TableHeap;
    use crate::types_db::value::Value;

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
                },
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
        let col1 = Column::new("a".parse().unwrap(), TypeId::VarChar);
        let col2 = Column::new("b".parse().unwrap(), TypeId::SmallInt);
        let col3 = Column::new("c".parse().unwrap(), TypeId::BigInt);
        let col4 = Column::new("d".parse().unwrap(), TypeId::Boolean);
        let col5 = Column::new("e".parse().unwrap(), TypeId::VarChar);
        let cols = vec![col1, col2, col3, col4, col5];
        let schema = Schema::new(cols);
        let tuple = construct_tuple(&schema);

        // Create transaction
        let disk_manager = Arc::new(DiskManager::new("tuple_test.db", "tuple_test.log"));
        let disk_scheduler = Arc::new(DiskScheduler::new(disk_manager.clone()));
        let replacer = Arc::new(Mutex::new(LRUKReplacer::new(10, 10)));
        let buffer_pool_manager = BufferPoolManager::new(100, disk_scheduler, disk_manager, replacer.clone());

        let table_latch = ReaderWriterLatch::new();
        let mut table = TableHeap::new(Arc::new(Mutex::new(buffer_pool_manager)), table_latch);

        let mut rid_v = vec![];
        for _ in 0..5000 {
            let rid = table.insert_tuple(TupleMeta::new(0, false), &tuple, None, None, 0);
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