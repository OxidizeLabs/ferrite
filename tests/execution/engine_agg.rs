use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn aggregations_and_group_by() {
	init_test_logger();
	let db = new_temp_db().await.unwrap();
	let mut w = CliResultWriter::new();

	db.execute_sql(
		"CREATE TABLE t (k INTEGER, v INTEGER);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	db.execute_sql(
		"INSERT INTO t VALUES (1, 10), (1, 20), (2, 1), (2, 2);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	db.execute_sql(
		"SELECT k, COUNT(*), SUM(v), AVG(v) FROM t GROUP BY k ORDER BY k;",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();
}
