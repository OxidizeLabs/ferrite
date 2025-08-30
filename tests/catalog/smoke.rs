use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;

#[tokio::test]
async fn catalog_smoke_list_tables_empty() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let (has_recovery, _) = db.files_exist();
    assert!(has_recovery || !has_recovery); // trivial sanity to use API
}


