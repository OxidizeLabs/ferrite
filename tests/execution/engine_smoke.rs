use crate::common::logger::init_test_logger;
use crate::common::tempdb::{cleanup_temp_artifacts, new_temp_db};
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn engine_smoke_ddl_dml_select() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    db.execute_sql(
        "CREATE TABLE users (id INTEGER, name VARCHAR);",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    db.execute_sql(
        "INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob');",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    db.execute_sql(
        "SELECT id, name FROM users ORDER BY id;",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();

    cleanup_temp_artifacts();
}

#[tokio::test]
async fn engine_smoke_aggregations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    db.execute_sql(
        "CREATE TABLE t (v INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    db.execute_sql(
        "INSERT INTO t VALUES (1), (2), (2), (3);",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    db.execute_sql(
        "SELECT COUNT(*), SUM(v) FROM t;",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    cleanup_temp_artifacts();
}

#[tokio::test]
async fn engine_smoke_joins() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    db.execute_sql(
        "CREATE TABLE a (id INTEGER, v INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    db.execute_sql(
        "CREATE TABLE b (id INTEGER, v INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    db.execute_sql(
        "INSERT INTO a VALUES (1, 10), (2, 20);",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    db.execute_sql(
        "INSERT INTO b VALUES (1, 100), (3, 300);",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();
    db.execute_sql(
        "SELECT a.id, a.v, b.v FROM a JOIN b ON a.id = b.id;",
        IsolationLevel::ReadCommitted,
        &mut w,
    )
    .await
    .unwrap();

    cleanup_temp_artifacts();
}
