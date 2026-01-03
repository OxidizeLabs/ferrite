#![allow(
    clippy::needless_borrow,
    clippy::clone_on_copy,
    clippy::unnecessary_mut_passed
)]

use std::sync::Arc;

use ferrite::buffer::buffer_pool_manager_async::BufferPoolManager;
use ferrite::buffer::lru_k_replacer::LRUKReplacer;
use ferrite::catalog::column::Column;
use ferrite::catalog::schema::Schema;
use ferrite::common::rid::RID;
use ferrite::storage::disk::async_disk::{AsyncDiskManager, DiskManagerConfig};
use ferrite::storage::table::table_heap::TableHeap;
use ferrite::storage::table::tuple::{Tuple, TupleMeta};
use ferrite::types_db::type_id::TypeId;
use ferrite::types_db::value::Value;
use parking_lot::RwLock;
use tempfile::TempDir;

use crate::common::logger::init_test_logger;

struct StorageTestContext {
    bpm: Arc<BufferPoolManager>,
    _temp_dir: TempDir,
}

impl StorageTestContext {
    async fn new(name: &str) -> Self {
        init_test_logger();
        const BUFFER_POOL_SIZE: usize = 100;
        const K: usize = 2;
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir
            .path()
            .join(format!("{name}.db"))
            .to_string_lossy()
            .to_string();
        let log_path = temp_dir
            .path()
            .join(format!("{name}.log"))
            .to_string_lossy()
            .to_string();
        let disk = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default())
            .await
            .unwrap();
        let replacer = Arc::new(RwLock::new(LRUKReplacer::new(7, K)));
        let bpm =
            Arc::new(BufferPoolManager::new(BUFFER_POOL_SIZE, Arc::new(disk), replacer).unwrap());
        Self {
            bpm,
            _temp_dir: temp_dir,
        }
    }

    fn create_table_heap(&self) -> TableHeap {
        TableHeap::new(self.bpm.clone(), 1)
    }
}

fn create_test_schema() -> Schema {
    Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
        Column::new("age", TypeId::Integer),
    ])
}

fn create_test_tuple(schema: &Schema) -> (TupleMeta, Tuple) {
    let tuple_values = vec![Value::new(1), Value::new("Alice"), Value::new(25)];
    let tuple = Tuple::new(&tuple_values, &schema, RID::new(0, 0));
    let meta = TupleMeta::new(0);
    (meta, tuple)
}

#[tokio::test]
async fn storage_basic_insert_and_get() {
    let ctx = StorageTestContext::new("storage_basic_insert_and_get").await;
    let table_heap = ctx.create_table_heap();
    let schema = create_test_schema();
    let (meta, tuple) = create_test_tuple(&schema);
    let rid = table_heap
        .insert_tuple(Arc::new(meta.clone()), &tuple)
        .expect("insert failed");
    let (stored_meta, stored_tuple) = table_heap.get_tuple(rid).expect("get failed");
    assert_eq!(stored_meta, Arc::new(meta));
    assert_eq!(stored_tuple, Arc::new(tuple));
}

#[tokio::test]
async fn page_management_links_and_counts() {
    use std::collections::HashSet;
    let ctx = StorageTestContext::new("page_management_links_and_counts").await;
    let table_heap = ctx.create_table_heap();
    let schema = create_test_schema();

    let mut page_ids = HashSet::new();
    let mut rids = Vec::new();

    for i in 0..61 {
        let values = vec![
            Value::new(i),
            Value::new(format!(
                "name{}_with_some_padding_to_ensure_large_enough_tuple",
                i
            )),
            Value::new(20 + i),
        ];
        let meta = Arc::new(TupleMeta::new(0));
        let rid = table_heap
            .insert_tuple_from_values(values, &schema, meta)
            .expect("insert");
        page_ids.insert(rid.get_page_id());
        rids.push(rid);
    }

    let mut page_ids: Vec<_> = page_ids.into_iter().collect();
    page_ids.sort();

    for window in page_ids.windows(2) {
        let current_page_id = window[0];
        let next_page_id = window[1];

        let current_guard = table_heap.get_page(current_page_id).expect("cur page");
        let current_page = current_guard.read();
        assert_eq!(current_page.get_next_page_id(), next_page_id);

        let next_guard = table_heap.get_page(next_page_id).expect("next page");
        let next_page = next_guard.read();
        assert_eq!(next_page.get_prev_page_id(), current_page_id);
    }

    assert_eq!(table_heap.get_first_page_id(), page_ids[0]);
    assert_eq!(table_heap.get_last_page_id(), *page_ids.last().unwrap());
}

#[tokio::test]
async fn page_overflow_rejected() {
    let ctx = StorageTestContext::new("page_overflow_rejected").await;
    let table_heap = ctx.create_table_heap();
    let schema = create_test_schema();

    let large_string = "x".repeat(4000);
    let values = vec![Value::new(1), Value::new(large_string), Value::new(25)];
    let tuple = Tuple::new(&values, &schema, RID::new(0, 0));
    let meta = TupleMeta::new(0);

    let result = table_heap.insert_tuple(Arc::new(meta), &tuple);
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("too large"));
}

#[tokio::test]
async fn tuple_update_and_readback() {
    let ctx = StorageTestContext::new("tuple_update_and_readback").await;
    let table_heap = ctx.create_table_heap();
    let schema = create_test_schema();

    let (meta, tuple) = create_test_tuple(&schema);
    let rid = table_heap
        .insert_tuple(Arc::new(meta.clone()), &tuple)
        .expect("insert");

    let updated_values = vec![Value::new(1), Value::new("Bob"), Value::new(30)];
    let updated_tuple = Tuple::new(&updated_values, &schema, rid);
    let updated_rid = table_heap
        .update_tuple(Arc::new(meta), &updated_tuple, rid, None)
        .expect("update");

    let (_, retrieved_tuple) = table_heap.get_tuple(updated_rid).expect("get");
    assert_eq!(retrieved_tuple.get_value(1), Value::new("Bob"));
    assert_eq!(retrieved_tuple.get_value(2), Value::new(30));
}

#[tokio::test]
async fn tuple_meta_update_applies() {
    let ctx = StorageTestContext::new("tuple_meta_update_applies").await;
    let table_heap = ctx.create_table_heap();
    let schema = create_test_schema();

    let (mut meta, mut tuple) = create_test_tuple(&schema);
    let rid = table_heap
        .insert_tuple(Arc::new(meta.clone()), &mut tuple)
        .expect("insert");

    meta.set_commit_timestamp(100);
    table_heap
        .update_tuple_meta(Arc::new(meta), rid)
        .expect("meta update");
    let retrieved_meta = table_heap.get_tuple_meta(rid).expect("get meta");
    assert_eq!(retrieved_meta.get_commit_timestamp(), Some(100));
}

#[tokio::test]
async fn page_iteration_counts_all() {
    let ctx = StorageTestContext::new("page_iteration_counts_all").await;
    let table_heap = ctx.create_table_heap();
    let schema = create_test_schema();

    let mut rids = Vec::new();
    for i in 0..5 {
        let values = vec![
            Value::new(i),
            Value::new(format!("name{}", i)),
            Value::new(20 + i),
        ];
        let meta = Arc::new(TupleMeta::new(0));
        let rid = table_heap
            .insert_tuple_from_values(values, &schema, meta)
            .expect("insert");
        rids.push(rid);
    }

    let mut found_count = 0;
    let mut current_page_id = table_heap.get_first_page_id();
    while current_page_id != ferrite::common::config::INVALID_PAGE_ID {
        let page_guard = table_heap.get_page(current_page_id).expect("page");
        let page = page_guard.read();
        found_count += page.get_num_tuples() as usize;
        current_page_id = page.get_next_page_id();
    }
    assert_eq!(found_count, rids.len());
}

#[tokio::test]
async fn concurrent_access_reads_ok() {
    use std::thread;
    let ctx = StorageTestContext::new("concurrent_access_reads_ok").await;
    let table_heap = Arc::new(ctx.create_table_heap());
    let schema = create_test_schema();

    let (meta, tuple) = create_test_tuple(&schema);
    let rid = table_heap
        .insert_tuple(Arc::new(meta), &tuple)
        .expect("insert");

    let mut handles = vec![];
    for _ in 0..5 {
        let table_heap = table_heap.clone();
        let handle = thread::spawn(move || {
            let result = table_heap.get_tuple(rid);
            assert!(result.is_ok());
        });
        handles.push(handle);
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[tokio::test]
async fn invalid_operations_error() {
    let ctx = StorageTestContext::new("invalid_operations_error").await;
    let table_heap = ctx.create_table_heap();
    let schema = create_test_schema();

    let invalid_rid = RID::new(999, 0);
    assert!(table_heap.get_tuple(invalid_rid).is_err());

    let (meta, tuple) = create_test_tuple(&schema);
    assert!(
        table_heap
            .update_tuple(Arc::new(meta.clone()), &tuple, invalid_rid, None)
            .is_err()
    );
    assert!(
        table_heap
            .update_tuple_meta(Arc::new(meta), invalid_rid)
            .is_err()
    );
}

#[tokio::test]
async fn page_boundary_conditions_size_checks() {
    let ctx = StorageTestContext::new("page_boundary_conditions_size_checks").await;
    let table_heap = ctx.create_table_heap();
    let schema = create_test_schema();

    let large_string = "x".repeat(3000);
    let values_ok = vec![Value::new(1), Value::new(large_string), Value::new(25)];
    let tuple_ok = Tuple::new(&values_ok, &schema, RID::new(0, 0));
    let meta = TupleMeta::new(0);
    assert!(
        table_heap
            .insert_tuple(Arc::new(meta.clone()), &tuple_ok)
            .is_ok()
    );

    let too_large_string = "x".repeat(4000);
    let values_big = vec![Value::new(1), Value::new(too_large_string), Value::new(25)];
    let tuple_big = Tuple::new(&values_big, &schema, RID::new(0, 0));
    assert!(table_heap.insert_tuple(Arc::new(meta), &tuple_big).is_err());
}

#[tokio::test]
async fn insert_tuple_from_values_variants() {
    let ctx = StorageTestContext::new("insert_tuple_from_values_variants").await;
    let table_heap = ctx.create_table_heap();

    let schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
        Column::new("age", TypeId::Integer),
    ]);

    let rid = table_heap
        .insert_tuple_from_values(
            vec![Value::new(1), Value::new("Alice"), Value::new(25)],
            &schema,
            Arc::new(TupleMeta::new(0)),
        )
        .expect("insert");
    let (_, tuple) = table_heap.get_tuple(rid).expect("get");
    assert_eq!(tuple.get_value(0), Value::new(1));
    assert_eq!(tuple.get_value(1), Value::new("Alice"));
    assert_eq!(tuple.get_value(2), Value::new(25));
}

#[tokio::test]
async fn insert_multiple_from_values() {
    let ctx = StorageTestContext::new("insert_multiple_from_values").await;
    let table_heap = ctx.create_table_heap();

    let schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
        Column::new("age", TypeId::Integer),
    ]);

    let mut rids = Vec::new();
    for i in 0..10 {
        let values = vec![
            Value::new(i),
            Value::new(format!("Name_{}", i)),
            Value::new(20 + i),
        ];
        let rid = table_heap
            .insert_tuple_from_values(values, &schema, Arc::new(TupleMeta::new(0)))
            .expect("insert");
        rids.push(rid);
    }

    for (i, rid) in rids.iter().enumerate() {
        let (_, tuple) = table_heap.get_tuple(*rid).expect("get");
        assert_eq!(tuple.get_value(0), Value::new(i as i32));
        assert_eq!(tuple.get_value(1), Value::new(format!("Name_{}", i)));
        assert_eq!(tuple.get_value(2), Value::new(20 + i as i32));
    }
}

#[tokio::test]
async fn insert_across_pages_with_large_rows() {
    use std::collections::HashSet;
    let ctx = StorageTestContext::new("insert_across_pages_with_large_rows").await;
    let table_heap = ctx.create_table_heap();

    let schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
        Column::new("data", TypeId::VarChar),
    ]);

    let mut rids = Vec::new();
    let mut page_ids = HashSet::new();
    for i in 0..50 {
        let large_data = format!(
            "This is a somewhat larger string that will help fill pages more quickly when we insert multiple tuples. Tuple index: {}",
            i
        );
        let values = vec![
            Value::new(i),
            Value::new(format!("Name_{}", i)),
            Value::new(large_data),
        ];
        let rid = table_heap
            .insert_tuple_from_values(values, &schema, Arc::new(TupleMeta::new(0)))
            .expect("insert");
        rids.push(rid);
        page_ids.insert(rid.get_page_id());
    }

    assert!(page_ids.len() > 1);

    for (i, rid) in rids.iter().enumerate() {
        let (_, tuple) = table_heap.get_tuple(*rid).expect("get");
        assert_eq!(tuple.get_value(0), Value::new(i as i32));
        assert_eq!(tuple.get_value(1), Value::new(format!("Name_{}", i)));
        let expected_data = format!(
            "This is a somewhat larger string that will help fill pages more quickly when we insert multiple tuples. Tuple index: {}",
            i
        );
        assert_eq!(tuple.get_value(2), Value::new(expected_data));
    }
}

#[tokio::test]
async fn insert_tuple_too_large_rejected() {
    let ctx = StorageTestContext::new("insert_tuple_too_large_rejected").await;
    let table_heap = ctx.create_table_heap();

    let schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("huge_data", TypeId::VarChar),
    ]);

    let huge_string = "x".repeat(5000);
    let values = vec![Value::new(1), Value::new(huge_string)];
    let result = table_heap.insert_tuple_from_values(values, &schema, Arc::new(TupleMeta::new(0)));
    assert!(result.is_err());
    assert!(result.unwrap_err().contains("too large"));
}
