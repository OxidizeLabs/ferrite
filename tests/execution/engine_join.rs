use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn basic_inner_join() {
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
}
