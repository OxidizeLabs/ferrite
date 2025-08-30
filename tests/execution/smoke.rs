use crate::common::logger::init_test_logger;
use crate::common::tempdb::{new_temp_db, cleanup_temp_artifacts};
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn execution_engine_smoke_create_insert_select() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    db.execute_sql(
        "CREATE TABLE t (id INTEGER, name VARCHAR);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await.unwrap();

    db.execute_sql(
        "INSERT INTO t VALUES (1, 'alice'), (2, 'bob');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await.unwrap();

    db.execute_sql(
        "SELECT * FROM t ORDER BY id;",
        IsolationLevel::ReadCommitted,
        &mut writer,
    ).await.unwrap();

    cleanup_temp_artifacts();
}


