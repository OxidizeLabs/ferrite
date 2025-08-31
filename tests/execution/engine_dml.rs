use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn insert_update_delete_basic() {
	init_test_logger();
	let db = new_temp_db().await.unwrap();
	let mut w = CliResultWriter::new();

	// Create table and insert rows
	db.execute_sql(
		"CREATE TABLE t (id INTEGER, v INTEGER);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	db.execute_sql(
		"INSERT INTO t VALUES (1, 10), (2, 20), (3, 30);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	// Update a subset
	db.execute_sql(
		"UPDATE t SET v = v + 1 WHERE id >= 2;",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	// Delete a subset
	db.execute_sql(
		"DELETE FROM t WHERE id = 1;",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	// Final select to ensure table operational
	db.execute_sql(
		"SELECT id, v FROM t ORDER BY id;",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();
}
