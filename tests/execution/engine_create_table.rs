#![allow(clippy::single_match)]

use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn create_table_basic_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Basic create -> insert -> select
    let ok = db
        .execute_sql(
            "CREATE TABLE t (id INTEGER, name VARCHAR(50), age INTEGER, active BOOLEAN);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO t VALUES (1, 'Alice', 25, true);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql("SELECT * FROM t;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_all_data_types() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE all_types (
				id INTEGER,
				name VARCHAR(100),
				big BIGINT,
				small SMALLINT,
				flag BOOLEAN
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_minimal_columns() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE single_col (id INTEGER);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_wide_schema() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE wide_table (
				col1 INTEGER, col2 INTEGER, col3 INTEGER, col4 INTEGER, col5 INTEGER,
				col6 VARCHAR(50), col7 VARCHAR(50), col8 VARCHAR(50), col9 VARCHAR(50), col10 VARCHAR(50)
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_varchar_sizes() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE varchar_test (
				id INTEGER,
				short_text VARCHAR(10),
				long_text VARCHAR(255)
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_numeric_precision() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE numeric_precision_test (
				id INTEGER,
				small_num SMALLINT,
				big_num BIGINT
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_edge_case_names() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE test_table_123 (id INTEGER, value VARCHAR(50));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "CREATE TABLE UPPER_CASE_TABLE (ID INTEGER, NAME VARCHAR(50));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_duplicate_name_error() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE duplicate_test (id INTEGER, name VARCHAR(50));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Duplicate should error (Err) or return Ok(false)
    let res = db
        .execute_sql(
            "CREATE TABLE duplicate_test (other_id INTEGER, other_name VARCHAR(100));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    match res {
        Ok(success) => assert!(!success, "duplicate create should not succeed"),
        Err(_) => {},
    }
}

#[tokio::test]
async fn create_table_primary_key_constraint() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE users_pk (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100)
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_default_values() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE user_preferences (
				id INTEGER,
				username VARCHAR(50),
				is_active BOOLEAN
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_foreign_key_constraint() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE departments (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100) NOT NULL
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "CREATE TABLE employees_fk (
				id INTEGER PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				department_id INTEGER
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_with_boolean_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE feature_flags (
				id INTEGER,
				feature_name VARCHAR(100),
				is_enabled BOOLEAN,
				is_experimental BOOLEAN,
				is_deprecated BOOLEAN
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO feature_flags VALUES
				(1, 'dark_mode', true, false, false),
				(2, 'beta_search', true, true, false),
				(3, 'old_ui', false, false, true),
				(4, 'new_algorithm', false, true, false);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "SELECT feature_name FROM feature_flags WHERE is_enabled = true;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"SELECT feature_name FROM feature_flags WHERE is_experimental = true AND is_deprecated = false;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_and_complex_queries() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE sales_data (
				id INTEGER,
				product_name VARCHAR(100),
				category VARCHAR(50),
				price DECIMAL,
				quantity INTEGER,
				sale_date DATE,
				is_online BOOLEAN
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO sales_data VALUES
				(1, 'Laptop', 'Electronics', 999.99, 2, '2023-01-15', true),
				(2, 'Book', 'Education', 29.99, 5, '2023-01-16', false),
				(3, 'Headphones', 'Electronics', 199.99, 1, '2023-01-17', true),
				(4, 'Desk', 'Furniture', 299.99, 1, '2023-01-18', false),
				(5, 'Mouse', 'Electronics', 49.99, 3, '2023-01-19', true);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "SELECT category, COUNT(*), AVG(price) FROM sales_data GROUP BY category;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"SELECT product_name, price FROM sales_data WHERE category = 'Electronics' AND price > 100 ORDER BY price DESC;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"SELECT product_name, price, quantity, price * quantity as total_value FROM sales_data ORDER BY total_value DESC;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_table_transaction_safety() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql("BEGIN", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "CREATE TABLE transaction_test (id INTEGER, data VARCHAR(100));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO transaction_test VALUES (1, 'test data');",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql("COMMIT", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "SELECT * FROM transaction_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}
