use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use ferrite::common::result_writer::CliResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_basic_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    let create_sql = "CREATE TABLE employees (
        id INTEGER,
        name VARCHAR(50),
        age INTEGER,
        salary BIGINT,
        active BOOLEAN
    )";
    db.execute_sql(create_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Insert test data
    let insert_sql = "INSERT INTO employees VALUES
        (1, 'Alice', 25, 50000, true),
        (2, 'Bob', 30, 60000, true),
        (3, 'Charlie', 35, 70000, false),
        (4, 'David', 40, 80000, true)";
    db.execute_sql(insert_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 1: Basic UPDATE with single column
    let update_sql = "UPDATE employees SET age = 26 WHERE id = 1";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Basic update operation failed");

    // Verify the update
    let select_sql = "SELECT age FROM employees WHERE id = 1";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 2: UPDATE multiple columns
    let update_sql = "UPDATE employees SET name = 'Alice Smith', salary = 55000 WHERE id = 1";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Multi-column update operation failed");

    // Verify the multi-column update
    let select_sql = "SELECT name, salary FROM employees WHERE id = 1";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 3: UPDATE with boolean value
    let update_sql = "UPDATE employees SET active = false WHERE id = 2";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Boolean update operation failed");

    // Verify boolean update
    let select_sql = "SELECT active FROM employees WHERE id = 2";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_with_conditions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    let create_sql = "CREATE TABLE employees (
        id INTEGER,
        name VARCHAR(50),
        age INTEGER,
        salary BIGINT,
        department VARCHAR(50)
    )";
    db.execute_sql(create_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Insert test data
    let insert_sql = "INSERT INTO employees VALUES
        (1, 'Alice', 25, 50000, 'Engineering'),
        (2, 'Bob', 30, 60000, 'Sales'),
        (3, 'Charlie', 35, 70000, 'Engineering'),
        (4, 'David', 40, 80000, 'Marketing'),
        (5, 'Eve', 28, 55000, 'Sales')";
    db.execute_sql(insert_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 1: UPDATE with range condition
    let update_sql = "UPDATE employees SET salary = salary + 5000 WHERE age > 30";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Range condition update failed");

    // Verify employees over 30 got salary increase
    let select_sql = "SELECT name, salary FROM employees WHERE age > 30 ORDER BY name";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 2: UPDATE with string condition
    let update_sql = "UPDATE employees SET salary = salary * 1.1 WHERE department = 'Engineering'";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "String condition update failed");

    // Verify Engineering employees got 10% raise
    let select_sql =
        "SELECT name, salary FROM employees WHERE department = 'Engineering' ORDER BY name";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 3: UPDATE with multiple conditions (AND)
    let update_sql = "UPDATE employees SET age = age + 1 WHERE department = 'Sales' AND age < 35";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Multiple condition update failed");

    // Verify only Bob and Eve (Sales, age < 35) got age increment
    let select_sql = "SELECT name, age FROM employees WHERE department = 'Sales' ORDER BY name";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_with_expressions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    let create_sql = "CREATE TABLE employees (
        id INTEGER,
        name VARCHAR(50),
        base_salary BIGINT,
        bonus BIGINT,
        years_experience INTEGER
    )";
    db.execute_sql(create_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Insert test data
    let insert_sql = "INSERT INTO employees VALUES
        (1, 'Alice', 50000, 5000, 3),
        (2, 'Bob', 60000, 8000, 5),
        (3, 'Charlie', 70000, 10000, 7)";
    db.execute_sql(insert_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 1: UPDATE with arithmetic expression
    let update_sql = "UPDATE employees SET base_salary = base_salary + (years_experience * 1000)";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Arithmetic expression update failed");

    // Verify the arithmetic updates
    let select_sql = "SELECT id, name, base_salary FROM employees ORDER BY id";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 2: UPDATE with column reference in expression
    let update_sql = "UPDATE employees SET bonus = base_salary * 0.1 WHERE years_experience >= 5";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Column reference expression update failed");

    // Verify bonus updates for employees with 5+ years experience
    let select_sql = "SELECT name, bonus FROM employees WHERE years_experience >= 5 ORDER BY name";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_no_rows_affected() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    let create_sql = "CREATE TABLE employees (
        id INTEGER,
        name VARCHAR(50),
        age INTEGER
    )";
    db.execute_sql(create_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Insert test data
    let insert_sql = "INSERT INTO employees VALUES
        (1, 'Alice', 25),
        (2, 'Bob', 30)";
    db.execute_sql(insert_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test UPDATE with condition that matches no rows
    let update_sql = "UPDATE employees SET age = 35 WHERE id = 999";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Should return false since no rows were affected
    assert!(!success, "UPDATE with no matching rows should return false");

    // Verify no data was changed
    let select_sql = "SELECT id, age FROM employees ORDER BY id";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_all_rows() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    let create_sql = "CREATE TABLE employees (
        id INTEGER,
        name VARCHAR(50),
        status VARCHAR(20)
    )";
    db.execute_sql(create_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Insert test data
    let insert_sql = "INSERT INTO employees VALUES
        (1, 'Alice', 'active'),
        (2, 'Bob', 'inactive'),
        (3, 'Charlie', 'pending')";
    db.execute_sql(insert_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test UPDATE without WHERE clause (affects all rows)
    let update_sql = "UPDATE employees SET status = 'updated'";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "UPDATE all rows should succeed");

    // Verify all rows were updated
    let select_sql = "SELECT status FROM employees";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_update_in_transaction() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    let create_sql = "CREATE TABLE accounts (
        id INTEGER,
        name VARCHAR(50),
        balance BIGINT
    )";
    db.execute_sql(create_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Insert test data
    let insert_sql = "INSERT INTO accounts VALUES
        (1, 'Alice', 1000),
        (2, 'Bob', 500)";
    db.execute_sql(insert_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 1: UPDATE in transaction with commit
    let begin_sql = "BEGIN";
    db.execute_sql(begin_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Transfer money from Alice to Bob
    let update_sql1 = "UPDATE accounts SET balance = balance - 200 WHERE id = 1";
    let success = db
        .execute_sql(update_sql1, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "First update in transaction failed");

    let update_sql2 = "UPDATE accounts SET balance = balance + 200 WHERE id = 2";
    let success = db
        .execute_sql(update_sql2, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Second update in transaction failed");

    // Commit transaction
    let commit_sql = "COMMIT";
    db.execute_sql(commit_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Verify changes were committed
    let select_sql = "SELECT balance FROM accounts WHERE id = 1";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    let select_sql = "SELECT balance FROM accounts WHERE id = 2";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Test 2: UPDATE in transaction with rollback
    let begin_sql = "BEGIN";
    db.execute_sql(begin_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Try to transfer more money
    let update_sql = "UPDATE accounts SET balance = balance - 500 WHERE id = 1";
    let success = db
        .execute_sql(update_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
    assert!(success, "Update in transaction failed");

    // Rollback transaction
    let rollback_sql = "ROLLBACK";
    db.execute_sql(rollback_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();

    // Verify changes were rolled back
    let select_sql = "SELECT balance FROM accounts WHERE id = 1";
    db.execute_sql(select_sql, IsolationLevel::ReadCommitted, &mut writer)
        .await
        .unwrap();
}
