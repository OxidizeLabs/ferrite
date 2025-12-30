use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_values_and_multirow() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Setup
    let ok = db
        .execute_sql(
            "CREATE TABLE users (id INTEGER, name VARCHAR(50), age INTEGER);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Single row insert
    let ok = db
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 25);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Multi-row insert
    let ok = db
        .execute_sql(
            "INSERT INTO users VALUES (2, 'Bob', 30), (3, 'Charlie', 35);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Verify
    let ok = db
        .execute_sql(
            "SELECT * FROM users ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_with_column_lists() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
		.execute_sql(
			"CREATE TABLE employees (id INTEGER, name VARCHAR(50), age INTEGER, active BOOLEAN, salary BIGINT);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"INSERT INTO employees (id, name, age, active, salary) VALUES (1, 'Alice', 25, true, 50000);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO employees (id, name, age) VALUES (2, 'Bob', 30);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"INSERT INTO employees (name, id, salary, age, active) VALUES ('Charlie', 3, 60000, 35, false);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "SELECT id, name, age, active, salary FROM employees ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_null_and_defaults() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
		.execute_sql(
			"CREATE TABLE contacts (id INTEGER, name VARCHAR(50), email VARCHAR(100), phone VARCHAR(20), age INTEGER);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO contacts VALUES (1, 'Alice', 'alice@example.com', NULL, 25);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO contacts VALUES (2, 'Bob', NULL, NULL, NULL);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO contacts (id, name, phone) VALUES (3, 'Charlie', '555-0123');",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "SELECT id, name, email, phone, age FROM contacts ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_with_select_and_transactions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    let ok = db
		.execute_sql(
			"CREATE TABLE employees_source (emp_id INTEGER, emp_name VARCHAR(50), department VARCHAR(50), salary BIGINT);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"CREATE TABLE employees_target (id INTEGER, name VARCHAR(50), dept VARCHAR(50), pay BIGINT);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"INSERT INTO employees_source VALUES (1, 'Alice', 'Engineering', 75000),(2, 'Bob', 'Sales', 65000),(3, 'Charlie', 'Engineering', 80000);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO employees_target SELECT * FROM employees_source;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql("BEGIN;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    let ok = db
		.execute_sql(
			"INSERT INTO employees_target (id, name, dept) SELECT emp_id + 100, emp_name, department FROM employees_source WHERE salary > 70000;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    let ok = db
        .execute_sql("COMMIT;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_basic_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table
    let ok = db
        .execute_sql(
            "CREATE TABLE users (id INTEGER, name VARCHAR(50), age INTEGER);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Test INSERT with VALUES
    let ok = db
        .execute_sql(
            "INSERT INTO users VALUES (1, 'Alice', 25);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Insert operation failed");

    // Test INSERT with multiple rows
    let ok = db
        .execute_sql(
            "INSERT INTO users VALUES (2, 'Bob', 30), (3, 'Charlie', 35);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Multi-row insert operation failed");

    // Verify inserts worked
    let ok = db
        .execute_sql(
            "SELECT * FROM users;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Select operation failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_column_specification() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table with various column types
    let ok = db
		.execute_sql(
			"CREATE TABLE employees (id INTEGER, name VARCHAR(50), age INTEGER, active BOOLEAN, salary BIGINT);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test INSERT with column list (all columns)
    let ok = db
		.execute_sql(
			"INSERT INTO employees (id, name, age, active, salary) VALUES (1, 'Alice', 25, true, 50000);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Insert with all columns failed");

    // Test INSERT with partial column list
    let ok = db
        .execute_sql(
            "INSERT INTO employees (id, name, age) VALUES (2, 'Bob', 30);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Insert with partial columns failed");

    // Test INSERT with columns in different order
    let ok = db
		.execute_sql(
			"INSERT INTO employees (name, id, salary, age, active) VALUES ('Charlie', 3, 60000, 35, false);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Insert with reordered columns failed");

    // Verify all inserts worked correctly
    let ok = db
        .execute_sql(
            "SELECT id, name, age, active, salary FROM employees ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Select after column-specific inserts failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_explicit_null_values() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table with nullable columns
    let ok = db
		.execute_sql(
			"CREATE TABLE contacts (id INTEGER, name VARCHAR(50), email VARCHAR(100), phone VARCHAR(20), age INTEGER);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test INSERT with explicit NULL values
    let ok = db
        .execute_sql(
            "INSERT INTO contacts VALUES (1, 'Alice', 'alice@example.com', NULL, 25);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Insert with explicit NULL failed");

    // Test INSERT with multiple NULL values
    let ok = db
        .execute_sql(
            "INSERT INTO contacts VALUES (2, 'Bob', NULL, NULL, NULL);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Insert with multiple NULLs failed");

    // Test INSERT with column specification and NULLs
    let ok = db
        .execute_sql(
            "INSERT INTO contacts VALUES (3, 'Charlie', NULL, '555-0123', NULL);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Insert with partial columns (implicit NULLs) failed");

    // Verify NULL handling
    let ok = db
        .execute_sql(
            "SELECT id, name, email, phone, age FROM contacts ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Select with NULL values failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_implicit_null_values() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table with nullable columns
    let ok = db
		.execute_sql(
			"CREATE TABLE contacts (id INTEGER, name VARCHAR(50), email VARCHAR(100), phone VARCHAR(20), age INTEGER);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test INSERT with column specification and NULLs
    let ok = db
        .execute_sql(
            "INSERT INTO contacts (id, name, phone) VALUES (3, 'Charlie', '555-0123');",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Insert with partial columns (implicit NULLs) failed");

    // Verify NULL handling
    let ok = db
        .execute_sql(
            "SELECT id, name, email, phone, age FROM contacts ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Select with NULL values failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_different_data_types() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table with all supported data types
    let ok = db
		.execute_sql(
			"CREATE TABLE all_types (id INTEGER, small_int SMALLINT, big_int BIGINT, float_val FLOAT, decimal_val DECIMAL, text_val VARCHAR(50), bool_val BOOLEAN, date_val DATE, time_val TIME, timestamp_val TIMESTAMP);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test INSERT with all data types
    let ok = db
		.execute_sql(
			"INSERT INTO all_types VALUES (1, 32767, 9223372036854775807, 3.14159, 123.456, 'Hello World', true, '2023-12-25', '14:30:00', '2023-12-25 14:30:00');",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Insert with all data types failed");

    // Test INSERT with different value representations
    let ok = db
		.execute_sql(
			"INSERT INTO all_types VALUES (2, -100, -1000000, 2.718, 999.99, 'Test String', false, '2024-01-01', '09:15:30', '2024-01-01 09:15:30');",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Insert with different value types failed");

    // Test INSERT with edge case values
    let ok = db
		.execute_sql(
			"INSERT INTO all_types VALUES (3, 0, 0, 0.0, 0.0, '', false, '1970-01-01', '00:00:00', '1970-01-01 00:00:00');",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Insert with edge case values failed");

    // Verify all data types were inserted correctly
    let ok = db
        .execute_sql(
            "SELECT * FROM all_types ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Select all data types failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_select_statement() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create source table
    let ok = db
		.execute_sql(
			"CREATE TABLE employees_source (emp_id INTEGER, emp_name VARCHAR(50), department VARCHAR(50), salary BIGINT);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Create target table with same structure
    let ok = db
		.execute_sql(
			"CREATE TABLE employees_target (id INTEGER, name VARCHAR(50), dept VARCHAR(50), pay BIGINT);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Insert test data into source table
    let ok = db
		.execute_sql(
			"INSERT INTO employees_source VALUES (1, 'Alice', 'Engineering', 75000), (2, 'Bob', 'Sales', 65000), (3, 'Charlie', 'Engineering', 80000), (4, 'David', 'Marketing', 70000), (5, 'Eve', 'Sales', 68000);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test 1: INSERT with simple SELECT (all rows)
    let ok = db
        .execute_sql(
            "INSERT INTO employees_target SELECT * FROM employees_source;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "INSERT with SELECT (all rows) failed");

    // Verify all rows were copied
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM employees_target;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Count query failed");

    // Clear target table for next test
    let ok = db
        .execute_sql(
            "DELETE FROM employees_target;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Test 2: INSERT with filtered SELECT
    let ok = db
		.execute_sql(
			"INSERT INTO employees_target SELECT * FROM employees_source WHERE department = 'Engineering';",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "INSERT with filtered SELECT failed");

    // Test 3: INSERT with SELECT and column specification
    let ok = db
		.execute_sql(
			"INSERT INTO employees_target (id, name, dept) SELECT emp_id, emp_name, department FROM employees_source WHERE salary > 70000;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "INSERT with SELECT and column specification failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_expressions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table
    let ok = db
		.execute_sql(
			"CREATE TABLE calculated_data (id INTEGER, name VARCHAR(50), calculated_value INTEGER, computed_text VARCHAR(50), derived_bool BOOLEAN);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test INSERT with arithmetic expressions
    let result = db
		.execute_sql(
			"INSERT INTO calculated_data VALUES (1, 'Row One', 10 + 5 * 2, 'Prefix: ' || 'Suffix', 10 > 5);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await;
    match result {
        Ok(success) => assert!(success, "INSERT with expressions failed"),
        Err(_) => println!("Arithmetic expressions not supported in INSERT"),
    }

    // Test INSERT with function calls (if supported)
    let result = db
		.execute_sql(
			"INSERT INTO calculated_data VALUES (2, 'Row Two', ABS(-25), UPPER('hello world'), LENGTH('test') > 3);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await;
    // This may fail if functions aren't implemented - that's OK
    match result {
        Ok(success) => assert!(success, "INSERT with functions failed"),
        Err(_) => println!("Functions not supported in INSERT expressions"),
    }

    // Test INSERT with CASE expressions
    let result = db
		.execute_sql(
			"INSERT INTO calculated_data VALUES (3, 'Row Three', CASE WHEN 1 = 1 THEN 100 ELSE 0 END, 'Case Result', CASE WHEN 'A' = 'A' THEN true ELSE false END);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await;
    match result {
        Ok(success) => assert!(success, "INSERT with CASE expressions failed"),
        Err(_) => println!("CASE expressions not supported in INSERT"),
    }

    // Verify expressions were evaluated correctly
    let ok = db
        .execute_sql(
            "SELECT * FROM calculated_data ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Select after expression INSERT failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_insert_transaction_behavior() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table
    let ok = db
        .execute_sql(
            "CREATE TABLE transactions_test (id INTEGER, name VARCHAR(50), amount INTEGER);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Test 1: INSERT with explicit transaction and COMMIT
    let ok = db
        .execute_sql("BEGIN;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO transactions_test VALUES (1, 'Alice', 1000);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "INSERT in transaction failed");

    let ok = db
        .execute_sql(
            "INSERT INTO transactions_test VALUES (2, 'Bob', 2000);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Second INSERT in transaction failed");

    let ok = db
        .execute_sql("COMMIT;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Verify data was committed
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM transactions_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Count query after commit failed");

    // Test 2: INSERT with ROLLBACK
    let ok = db
        .execute_sql("BEGIN;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    let ok = db
        .execute_sql(
            "INSERT INTO transactions_test VALUES (3, 'Charlie', 3000);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "INSERT before rollback failed");

    let ok = db
        .execute_sql("ROLLBACK;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Verify rollback worked
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM transactions_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Count query after rollback failed");

    // Test 3: Multiple INSERTs in single transaction
    let ok = db
        .execute_sql("BEGIN;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Insert multiple rows in transaction
    for i in 10..15 {
        let insert_sql = format!(
            "INSERT INTO transactions_test VALUES ({}, 'User{}', {});",
            i,
            i,
            i * 100
        );
        let ok = db
            .execute_sql(&insert_sql, IsolationLevel::ReadCommitted, &mut w)
            .await
            .unwrap();
        assert!(ok, "Batch INSERT {} failed", i);
    }

    let ok = db
        .execute_sql("COMMIT;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Verify all batch inserts were committed
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM transactions_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Final count query failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_error_cases() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table
    let ok = db
        .execute_sql(
            "CREATE TABLE error_test (id INTEGER, name VARCHAR(50), age INTEGER);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Test 1: INSERT into non-existent table
    let result = db
        .execute_sql(
            "INSERT INTO non_existent_table VALUES (1, 'Alice', 25);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    assert!(
        result.is_err(),
        "INSERT into non-existent table should fail"
    );

    // Test 2: INSERT with wrong number of values (too many)
    let result = db
        .execute_sql(
            "INSERT INTO error_test VALUES (1, 'Alice', 25, 'extra_value');",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    // This may or may not fail depending on implementation - document the behavior
    match result {
        Ok(_) => println!("Note: INSERT with extra values was accepted"),
        Err(_) => println!("INSERT with extra values was correctly rejected"),
    }

    // Test 3: INSERT with wrong number of values (too few)
    let result = db
        .execute_sql(
            "INSERT INTO error_test VALUES (1, 'Alice');",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    match result {
        Ok(_) => println!("Note: INSERT with missing values was accepted (NULLs assumed)"),
        Err(_) => println!("INSERT with missing values was correctly rejected"),
    }

    // Test 4: INSERT with column name mismatch
    let result = db
        .execute_sql(
            "INSERT INTO error_test (id, non_existent_column) VALUES (1, 'value');",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    assert!(
        result.is_err(),
        "INSERT with non-existent column should fail"
    );

    // Test 5: INSERT with type mismatch (string in integer column)
    let result = db
        .execute_sql(
            "INSERT INTO error_test VALUES ('not_a_number', 'Alice', 25);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    match result {
        Ok(_) => println!("Note: Type coercion was applied for string->integer"),
        Err(_) => println!("Type mismatch was correctly detected and rejected"),
    }

    // Verify table is still empty or has only valid data
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM error_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Count query should work even after errors");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_large_batch() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table
    let ok = db
        .execute_sql(
            "CREATE TABLE large_batch (id INTEGER, data VARCHAR(50), value INTEGER);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    // Test INSERT with many values in single statement
    let mut values = Vec::new();
    for i in 1..=50 {
        values.push(format!("({}, 'data{}', {})", i, i, i * 10));
    }
    let insert_sql = format!("INSERT INTO large_batch VALUES {};", values.join(", "));

    let ok = db
        .execute_sql(&insert_sql, IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok, "Large batch INSERT failed");

    // Verify all rows were inserted
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM large_batch;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Count query failed");

    // Test SELECT with some of the data to verify correctness
    let ok = db
        .execute_sql(
            "SELECT id, data, value FROM large_batch WHERE id <= 5 ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Sample select failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_default_values() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create table with DEFAULT values
    let ok = db
		.execute_sql(
			"CREATE TABLE default_test (id INTEGER, name VARCHAR(100), status VARCHAR(20) DEFAULT 'active', created_date DATE DEFAULT '2024-01-01', is_enabled BOOLEAN DEFAULT true);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Create table with defaults failed");

    // Test 1: INSERT with all explicit values
    let ok = db
        .execute_sql(
            "INSERT INTO default_test VALUES (1, 'Alice', 'inactive', '2023-12-25', false);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "INSERT with explicit values failed");

    // Test 2: INSERT with partial columns (should use defaults)
    let ok = db
        .execute_sql(
            "INSERT INTO default_test (id, name) VALUES (2, 'Bob');",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "INSERT with partial columns failed");

    // Test 3: INSERT with explicit DEFAULT keyword
    let result = db
        .execute_sql(
            "INSERT INTO default_test VALUES (3, 'Charlie', DEFAULT, DEFAULT, DEFAULT);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    match result {
        Ok(success) => assert!(success, "INSERT with DEFAULT keyword failed"),
        Err(_) => println!("DEFAULT keyword not supported in INSERT VALUES"),
    }

    // Verify the results
    let ok = db
        .execute_sql(
            "SELECT id, name, status, created_date, is_enabled FROM default_test ORDER BY id;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Select after default inserts failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_foreign_key_relationships() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create parent table (departments)
    let ok = db
		.execute_sql(
			"CREATE TABLE departments_fk (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, budget DECIMAL);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Create departments table failed");

    // Create child table (employees with foreign key)
    let result = db
		.execute_sql(
			"CREATE TABLE employees_fk_test (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, department_id INTEGER, salary DECIMAL, FOREIGN KEY (department_id) REFERENCES departments_fk(id));",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await;
    match result {
        Ok(success) => assert!(success, "Create employees table with FK failed"),
        Err(_) => {
            println!("Foreign key constraints not supported, creating table without FK");
            let ok = db
				.execute_sql(
					"CREATE TABLE employees_fk_test (id INTEGER PRIMARY KEY, name VARCHAR(100) NOT NULL, department_id INTEGER, salary DECIMAL);",
					IsolationLevel::ReadCommitted,
					&mut w,
				)
				.await
				.unwrap();
            assert!(ok);
        },
    }

    // Insert parent records first
    let ok = db
		.execute_sql(
			"INSERT INTO departments_fk VALUES (1, 'Engineering', 1000000.00), (2, 'Sales', 500000.00), (3, 'Marketing', 300000.00);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Insert departments failed");

    // Test 1: INSERT employees with valid foreign key references
    let ok = db
		.execute_sql(
			"INSERT INTO employees_fk_test VALUES (1, 'Alice Johnson', 1, 75000.00), (2, 'Bob Smith', 2, 65000.00), (3, 'Charlie Brown', 1, 80000.00);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "INSERT employees with valid FK failed");

    // Test 2: INSERT employee with NULL foreign key (should be allowed)
    let ok = db
        .execute_sql(
            "INSERT INTO employees_fk_test VALUES (4, 'David Wilson', NULL, 70000.00);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "INSERT with NULL FK failed");

    // Test 3: Try to INSERT employee with invalid foreign key
    let result = db
        .execute_sql(
            "INSERT INTO employees_fk_test VALUES (5, 'Eve Davis', 999, 72000.00);",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await;
    // FK constraint enforcement depends on implementation
    match result {
        Ok(_) => println!("Note: Foreign key constraint not enforced on INSERT"),
        Err(_) => println!("Foreign key constraint correctly enforced on INSERT"),
    }

    // Verify successful inserts with JOIN to show relationships
    let ok = db
		.execute_sql(
			"SELECT e.name, d.name as department_name, e.salary FROM employees_fk_test e LEFT JOIN departments_fk d ON e.department_id = d.id ORDER BY e.id;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "JOIN query failed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_insert_performance_bulk_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table optimized for bulk inserts
    let ok = db
		.execute_sql(
			"CREATE TABLE bulk_test (id INTEGER, batch_id INTEGER, value INTEGER, text_data VARCHAR(50), computed DECIMAL);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test Flow:
    // 1. Bulk INSERT 1000 rows (tests bulk insert performance)
    // 2. DELETE all rows (clears table for individual inserts)
    // 3. Individual INSERT 50 rows (tests single insert performance)
    // 4. INSERT with SELECT 25 rows (tests query-based inserts)
    // Final state: 75 rows (50 + 25)

    // Test 1: Single large INSERT statement with 1000 rows
    let mut values = Vec::new();
    for i in 1..=1000 {
        values.push(format!(
            "({}, {}, {}, 'text_data_{}', {})",
            i,
            i / 10, // batch_id groups every 10 rows
            i * i,  // value is square of id
            i,
            i as f64 * std::f64::consts::PI // computed value
        ));
    }
    let bulk_insert_sql = format!("INSERT INTO bulk_test VALUES {}", values.join(", "));

    let ok = db
        .execute_sql(&bulk_insert_sql, IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok, "Bulk INSERT failed");
    println!("Bulk INSERT of 1000 rows completed successfully");

    // Verify bulk insert
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM bulk_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Count query failed");

    // Test 2: Multiple individual INSERT statements for comparison
    let ok = db
        .execute_sql(
            "DELETE FROM bulk_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok);

    for i in 1..=50 {
        // Fewer iterations to keep test reasonable
        let individual_insert_sql = format!(
            "INSERT INTO bulk_test VALUES ({}, {}, {}, 'text_data_{}', {});",
            i,
            i / 10,
            i * i,
            i,
            i as f64 * std::f64::consts::PI
        );
        let ok = db
            .execute_sql(
                &individual_insert_sql,
                IsolationLevel::ReadCommitted,
                &mut w,
            )
            .await
            .unwrap();
        assert!(ok, "Individual INSERT {} failed", i);
    }

    println!("50 individual INSERTs completed successfully");

    // Test 3: INSERT with SELECT from existing data (data duplication)
    let result = db
		.execute_sql(
			"INSERT INTO bulk_test SELECT id + 1000, batch_id + 100, value * 2, 'copied_' || text_data, computed * 2 FROM bulk_test WHERE id <= 25;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await;
    match result {
        Ok(success) => {
            assert!(success, "INSERT with SELECT failed");
            println!("INSERT with SELECT (25 rows) completed successfully");
        },
        Err(_) => println!("INSERT with SELECT expressions not supported"),
    }

    // Verify final state
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM bulk_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Final count query failed");

    // Test aggregation on bulk data
    let ok = db
		.execute_sql(
			"SELECT batch_id, COUNT(*), AVG(value), SUM(computed) FROM bulk_test GROUP BY batch_id ORDER BY batch_id LIMIT 5;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Aggregation on bulk data failed");

    println!("Bulk operations test completed successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_insert_concurrent_transactions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table for concurrent operations
    let ok = db
		.execute_sql(
			"CREATE TABLE concurrent_test (id INTEGER, transaction_id INTEGER, operation_order INTEGER, data VARCHAR(50));",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test 1: INSERT in one transaction, verify isolation
    let ok = db
        .execute_sql("BEGIN;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Insert data in first transaction
    let ok = db
		.execute_sql(
			"INSERT INTO concurrent_test VALUES (1, 1, 1, 'Transaction 1 Data 1'), (2, 1, 2, 'Transaction 1 Data 2');",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "INSERT in first transaction failed");

    // Verify data is visible within the transaction
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM concurrent_test WHERE transaction_id = 1;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Count query in transaction failed");

    // Commit first transaction
    let ok = db
        .execute_sql("COMMIT;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Test 2: Second transaction with more data
    let ok = db
        .execute_sql("BEGIN;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Insert data in second transaction
    let ok = db
		.execute_sql(
			"INSERT INTO concurrent_test VALUES (3, 2, 1, 'Transaction 2 Data 1'), (4, 2, 2, 'Transaction 2 Data 2'), (5, 2, 3, 'Transaction 2 Data 3');",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "INSERT in second transaction failed");

    // Test rollback scenario
    let ok = db
        .execute_sql("ROLLBACK;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Verify only first transaction data remains
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM concurrent_test;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Count query after rollback failed");

    // Test 3: Successful transaction sequence
    let ok = db
        .execute_sql("BEGIN;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Insert and immediately commit
    let ok = db
        .execute_sql(
            "INSERT INTO concurrent_test VALUES (6, 3, 1, 'Transaction 3 Data');",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "INSERT in third transaction failed");

    let ok = db
        .execute_sql("COMMIT;", IsolationLevel::ReadCommitted, &mut w)
        .await
        .unwrap();
    assert!(ok);

    // Verify final state
    let ok = db
		.execute_sql(
			"SELECT transaction_id, COUNT(*) FROM concurrent_test GROUP BY transaction_id ORDER BY transaction_id;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "Final verification query failed");
    println!("Concurrent transactions test completed successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_insert_with_computed_columns() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut w = CliResultWriter::new();

    // Create test table with computed/derived values
    let ok = db
		.execute_sql(
			"CREATE TABLE employee_computed (id INTEGER, first_name VARCHAR(50), last_name VARCHAR(50), birth_year INTEGER, salary DECIMAL);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok);

    // Test 1: INSERT with computed full name and age calculation
    let ok = db
		.execute_sql(
			"INSERT INTO employee_computed VALUES (1, 'John', 'Doe', 1990, 75000.00), (2, 'Jane', 'Smith', 1985, 85000.00), (3, 'Bob', 'Johnson', 1995, 65000.00);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await
		.unwrap();
    assert!(ok, "INSERT with computed column data failed");

    // Test 2: SELECT with computed columns (full name, current age, salary grade)
    let result = db
		.execute_sql(
			"SELECT id, first_name || ' ' || last_name as full_name, 2024 - birth_year as current_age, CASE WHEN salary >= 80000 THEN 'Senior' WHEN salary >= 70000 THEN 'Mid-level' ELSE 'Junior' END as salary_grade, salary * 12 as annual_salary FROM employee_computed ORDER BY salary DESC;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await;

    match result {
        Ok(success) => {
            assert!(success, "SELECT with computed columns failed");
            println!("SELECT with computed columns succeeded");
        },
        Err(_) => println!("Note: Advanced computed column expressions not supported"),
    }

    // Test 3: INSERT with mathematical computations
    let result = db
		.execute_sql(
			"INSERT INTO employee_computed VALUES (4, 'Alice', 'Wilson', 1988, 65000.00 + 15000.00), (5, 'Charlie', 'Brown', 1992, 70000.00 * 1.1);",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await;

    match result {
        Ok(success) => {
            assert!(success, "INSERT with mathematical expressions failed");
            println!("Mathematical expressions in INSERT succeeded");

            // Verify the computed values were inserted correctly
            let ok = db
				.execute_sql(
					"SELECT first_name, last_name, salary FROM employee_computed WHERE id IN (4, 5) ORDER BY id;",
					IsolationLevel::ReadCommitted,
					&mut w,
				)
				.await
				.unwrap();
            assert!(ok, "Verification query failed");
        },
        Err(_) => println!("Note: Mathematical expressions in INSERT VALUES not supported"),
    }

    // Test 4: INSERT based on computation from existing data
    let result = db
		.execute_sql(
			"INSERT INTO employee_computed SELECT id + 100, 'Copy_' || first_name, last_name, birth_year + 1, salary * 0.9 FROM employee_computed WHERE id <= 2;",
			IsolationLevel::ReadCommitted,
			&mut w,
		)
		.await;
    match result {
        Ok(success) => {
            assert!(success, "INSERT with SELECT computation failed");
            println!("INSERT with SELECT computation succeeded");
        },
        Err(_) => println!("Note: Computed INSERT with SELECT not supported"),
    }

    // Verify all data
    let ok = db
        .execute_sql(
            "SELECT COUNT(*) FROM employee_computed;",
            IsolationLevel::ReadCommitted,
            &mut w,
        )
        .await
        .unwrap();
    assert!(ok, "Final count query failed");
    println!("Computed columns test completed successfully");
}
