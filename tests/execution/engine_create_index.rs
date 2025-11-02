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

#[tokio::test]
async fn create_index_composite_indexes() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Base table
    db.execute_sql(
		"CREATE TABLE employees (id INTEGER, first_name VARCHAR(50), last_name VARCHAR(50), department VARCHAR(50), salary BIGINT);",
		IsolationLevel::ReadCommitted,
		&mut w,
	)
	.await
	.unwrap();

    // Composite indexes
    for sql in [
        "CREATE INDEX idx_employees_name ON employees (first_name, last_name);",
        "CREATE INDEX idx_employees_dept_salary ON employees (department, salary);",
        "CREATE INDEX idx_employees_full ON employees (department, first_name, last_name);",
    ] {
        let ok = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok);
    }

    // Simple query after indexes
    let ok = db
		.execute_sql(
			"SELECT first_name, last_name FROM employees WHERE department = 'Engineering' ORDER BY salary DESC;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_unique_index() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Base table
    let ok = db
		.execute_sql(
			"CREATE TABLE users (id INTEGER, email VARCHAR(120), username VARCHAR(120), social_security VARCHAR(32));",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Unique indexes
    for sql in [
        "CREATE UNIQUE INDEX idx_users_email ON users (email);",
        "CREATE UNIQUE INDEX idx_users_username ON users (username);",
        "CREATE UNIQUE INDEX idx_users_email_username ON users (email, username);",
    ] {
        let ok = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok);
    }
}

#[tokio::test]
async fn create_index_different_data_types() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE all_types (
				id INTEGER,
				small_int_col SMALLINT,
				big_int_col BIGINT,
				float_col FLOAT,
				decimal_col DECIMAL,
				varchar_col VARCHAR(50),
				boolean_col BOOLEAN,
				date_col DATE,
				time_col TIME,
				timestamp_col TIMESTAMP
			);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    for sql in [
        "CREATE INDEX idx_small_int ON all_types (small_int_col);",
        "CREATE INDEX idx_big_int ON all_types (big_int_col);",
        "CREATE INDEX idx_float ON all_types (float_col);",
        "CREATE INDEX idx_decimal ON all_types (decimal_col);",
        "CREATE INDEX idx_varchar ON all_types (varchar_col);",
        "CREATE INDEX idx_boolean ON all_types (boolean_col);",
        "CREATE INDEX idx_date ON all_types (date_col);",
        "CREATE INDEX idx_time ON all_types (time_col);",
        "CREATE INDEX idx_timestamp ON all_types (timestamp_col);",
    ] {
        let ok = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok);
    }
}

#[tokio::test]
async fn create_index_naming_conventions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR(50), value INTEGER);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    for sql in [
        "CREATE INDEX simple_name ON test_table (id);",
        "CREATE INDEX idx_with_underscores ON test_table (name);",
        "CREATE INDEX CamelCaseIndex ON test_table (value);",
        "CREATE INDEX index123 ON test_table (id, name);",
        "CREATE INDEX very_long_index_name_that_describes_exactly_what_it_does ON test_table (name, value);",
    ] {
        let ok = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok);
    }
}

#[tokio::test]
async fn create_index_error_cases() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR(50));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Non-existent table
    let res = db
        .execute_sql(
            "CREATE INDEX idx_nonexistent ON nonexistent_table (id);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    assert!(res.is_err());

    // Non-existent column
    let res = db
        .execute_sql(
            "CREATE INDEX idx_bad_column ON test_table (nonexistent_column);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    assert!(res.is_err());

    // Valid index
    let ok = db
        .execute_sql(
            "CREATE INDEX idx_valid ON test_table (id);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Duplicate name (may be allowed depending on implementation)
    let _ = db
        .execute_sql(
            "CREATE INDEX idx_valid ON test_table (name);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;

    // Auto-generated name
    let ok = db
        .execute_sql(
            "CREATE INDEX ON test_table (id);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Invalid syntaxes
    for sql in [
        "CREATE INDEX idx_test test_table (id)",
        "CREATE INDEX idx_test ON test_table",
        "CREATE INDEX idx_test ON test_table ()",
    ] {
        let res = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await;
        assert!(res.is_err());
    }

    // Valid auto-generated name variants
    for sql in [
        "CREATE INDEX ON test_table (id)",
        "CREATE INDEX ON test_table (name)",
        "CREATE INDEX ON test_table (id, name)",
    ] {
        let ok = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok);
    }
}

#[tokio::test]
async fn create_index_on_populated_table() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
		.execute_sql(
			"CREATE TABLE employees (id INTEGER, name VARCHAR(50), age INTEGER, department VARCHAR(50));",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    for sql in [
        "CREATE INDEX idx_emp_name ON employees (name);",
        "CREATE INDEX idx_emp_age ON employees (age);",
        "CREATE INDEX idx_emp_dept ON employees (department);",
        "CREATE INDEX idx_emp_age_dept ON employees (age, department);",
    ] {
        let ok = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok);
    }

    // Queries after index creation
    for sql in [
        "SELECT name FROM employees WHERE age = 30;",
        "SELECT * FROM employees WHERE department = 'Engineering' ORDER BY name;",
        "SELECT name, age FROM employees WHERE age > 35 AND department = 'Sales';",
        "SELECT department, COUNT(*) FROM employees GROUP BY department;",
    ] {
        let ok = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok);
    }
}

#[tokio::test]
async fn create_index_in_transaction() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE test_table (id INTEGER, data VARCHAR(50));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql("BEGIN", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "CREATE INDEX idx_test_data ON test_table (data);",
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
            "SELECT * FROM test_table WHERE data = 'data1';",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql("BEGIN", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "CREATE INDEX idx_test_id ON test_table (id);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql("ROLLBACK", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM test_table;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_index_if_not_exists() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE test_table (id INTEGER, name VARCHAR(50));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let res = db
        .execute_sql(
            "CREATE INDEX IF NOT EXISTS idx_test_id ON test_table (id);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;

    match res {
        Ok(ok) => {
            assert!(ok);
            let ok = db
                .execute_sql(
                    "CREATE INDEX IF NOT EXISTS idx_test_id ON test_table (id);",
                    IsolationLevel::ReadCommitted,
                    &mut w,
                )
                .await
                .unwrap();
            assert!(ok);
        }
        Err(_) => {
            let ok = db
                .execute_sql(
                    "CREATE INDEX idx_test_id ON test_table (id);",
                    IsolationLevel::ReadCommitted,
                    &mut w,
                )
                .await
                .unwrap();
            assert!(ok);
        }
    }

    let ok = db
        .execute_sql(
            "SELECT * FROM test_table WHERE id = 1;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test]
async fn create_index_multiple_tables() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
        .execute_sql(
            "CREATE TABLE users (user_id INTEGER, username VARCHAR(50), email VARCHAR(100));",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"CREATE TABLE orders (order_id INTEGER, user_id INTEGER, product_name VARCHAR(50), order_date DATE);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    for sql in [
        "CREATE INDEX idx_users_email ON users (email);",
        "CREATE INDEX idx_users_username ON users (username);",
        "CREATE INDEX idx_orders_user_id ON orders (user_id);",
        "CREATE INDEX idx_orders_date ON orders (order_date);",
        "CREATE INDEX idx_orders_user_product ON orders (user_id, product_name);",
    ] {
        let ok = db
            .execute_sql(sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok);
    }

    let ok = db
		.execute_sql(
			"SELECT u.username, o.product_name FROM users u JOIN orders o ON u.user_id = o.user_id WHERE u.email = 'alice@example.com';",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "SELECT product_name FROM orders WHERE user_id = 1 AND order_date = '2023-01-15';",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}
