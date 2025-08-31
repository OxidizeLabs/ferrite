use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn create_index_basic_operations() {
	init_test_logger();
	let db = new_temp_db().await.unwrap();
	let mut w = CliResultWriter::new();

	// Create base table
	db.execute_sql(
		"CREATE TABLE items (id INTEGER, code VARCHAR(16), price INTEGER);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	// Create index on a single column
	db.execute_sql(
		"CREATE INDEX idx_items_code ON items(code);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	// Verify basic insert still works and index does not break DML
	db.execute_sql(
		"INSERT INTO items VALUES (1, 'A1', 100), (2, 'B2', 150);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

	// Simple query to ensure table is operational
	db.execute_sql(
		"SELECT id, code FROM items ORDER BY id;",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();
}
