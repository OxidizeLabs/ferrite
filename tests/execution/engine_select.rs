use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn select_where_order_by() {
	init_test_logger();
	let db = new_temp_db().await.unwrap();
	let mut w = CliResultWriter::new();

	db.execute_sql(
		"CREATE TABLE t (id INTEGER, name VARCHAR(32), v INTEGER);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	db.execute_sql(
		"INSERT INTO t VALUES (3, 'c', 30), (1, 'a', 10), (2, 'b', 20);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	db.execute_sql(
		"SELECT id, name FROM t WHERE v >= 20 ORDER BY id;",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();
}
