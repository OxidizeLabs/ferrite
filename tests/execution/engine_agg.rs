use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn test_count_aggregation() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    db.execute_sql(
        "CREATE TABLE employees (id INTEGER, name VARCHAR(50), department VARCHAR(50), salary INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO employees VALUES 
         (1, 'Alice', 'Engineering', 70000),
         (2, 'Bob', 'Sales', 50000),
         (3, 'Charlie', 'Engineering', 80000),
         (4, 'David', 'Sales', 55000),
         (5, 'Eve', 'Marketing', 60000);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test COUNT(*)
    let mut writer1 = CliResultWriter::new();
    db.execute_sql(
        "SELECT COUNT(*) FROM employees;",
        IsolationLevel::ReadCommitted,
        &mut writer1,
    )
    .await
    .unwrap();

    // Test COUNT(column)
    let mut writer2 = CliResultWriter::new();
    db.execute_sql(
        "SELECT COUNT(name) FROM employees;",
        IsolationLevel::ReadCommitted,
        &mut writer2,
    )
    .await
    .unwrap();

    // Test COUNT(DISTINCT column)
    let mut writer3 = CliResultWriter::new();
    db.execute_sql(
        "SELECT COUNT(DISTINCT department) FROM employees;",
        IsolationLevel::ReadCommitted,
        &mut writer3,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_sum_aggregation() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    db.execute_sql(
        "CREATE TABLE transactions (id INTEGER, amount INTEGER, category VARCHAR(10));",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO transactions VALUES 
         (1, 100, 'A'),
         (2, 200, 'B'),
         (3, 150, 'A'),
         (4, 300, 'B'),
         (5, 75, 'C');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test SUM
    let mut writer1 = CliResultWriter::new();
    db.execute_sql(
        "SELECT SUM(amount) FROM transactions;",
        IsolationLevel::ReadCommitted,
        &mut writer1,
    )
    .await
    .unwrap();

    // Test SUM with WHERE
    let mut writer2 = CliResultWriter::new();
    db.execute_sql(
        "SELECT SUM(amount) FROM transactions WHERE category = 'A';",
        IsolationLevel::ReadCommitted,
        &mut writer2,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_avg_aggregation() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    db.execute_sql(
        "CREATE TABLE grades (id INTEGER, score INTEGER, subject VARCHAR(20));",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO grades VALUES 
         (1, 85, 'Math'),
         (2, 92, 'Math'),
         (3, 78, 'Math'),
         (4, 88, 'Science'),
         (5, 94, 'Science');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test AVG
    let mut writer1 = CliResultWriter::new();
    db.execute_sql(
        "SELECT AVG(score) FROM grades;",
        IsolationLevel::ReadCommitted,
        &mut writer1,
    )
    .await
    .unwrap();

    // Test AVG with WHERE
    let mut writer2 = CliResultWriter::new();
    db.execute_sql(
        "SELECT AVG(score) FROM grades WHERE subject = 'Math';",
        IsolationLevel::ReadCommitted,
        &mut writer2,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_min_max_aggregation() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    db.execute_sql(
        "CREATE TABLE weather (id INTEGER, temperature INTEGER, city VARCHAR(20));",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO weather VALUES 
         (1, 22, 'NYC'),
         (2, 35, 'Phoenix'),
         (3, 18, 'Seattle'),
         (4, 28, 'Denver'),
         (5, 15, 'Boston');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test MIN
    let mut writer1 = CliResultWriter::new();
    db.execute_sql(
        "SELECT MIN(temperature) FROM weather;",
        IsolationLevel::ReadCommitted,
        &mut writer1,
    )
    .await
    .unwrap();

    // Test MAX
    let mut writer2 = CliResultWriter::new();
    db.execute_sql(
        "SELECT MAX(temperature) FROM weather;",
        IsolationLevel::ReadCommitted,
        &mut writer2,
    )
    .await
    .unwrap();

    // Test MIN and MAX together
    let mut writer3 = CliResultWriter::new();
    db.execute_sql(
        "SELECT MIN(temperature), MAX(temperature) FROM weather;",
        IsolationLevel::ReadCommitted,
        &mut writer3,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_aggregation_with_group_by() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    db.execute_sql(
        "CREATE TABLE employees (id INTEGER, department VARCHAR(20), salary INTEGER, bonus INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO employees VALUES 
         (1, 'Engineering', 70000, 5000),
         (2, 'Engineering', 80000, 6000),
         (3, 'Sales', 50000, 3000),
         (4, 'Sales', 55000, 4000),
         (5, 'Marketing', 60000, 4500);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test COUNT with GROUP BY
    let mut writer1 = CliResultWriter::new();
    db.execute_sql(
        "SELECT department, COUNT(*) FROM employees GROUP BY department;",
        IsolationLevel::ReadCommitted,
        &mut writer1,
    )
    .await
    .unwrap();

    // Test SUM with GROUP BY
    let mut writer2 = CliResultWriter::new();
    db.execute_sql(
        "SELECT department, SUM(salary) FROM employees GROUP BY department;",
        IsolationLevel::ReadCommitted,
        &mut writer2,
    )
    .await
    .unwrap();

    // Test AVG with GROUP BY
    let mut writer3 = CliResultWriter::new();
    db.execute_sql(
        "SELECT department, AVG(salary) FROM employees GROUP BY department;",
        IsolationLevel::ReadCommitted,
        &mut writer3,
    )
    .await
    .unwrap();

    // Test multiple aggregations with GROUP BY
    let mut writer4 = CliResultWriter::new();
    db.execute_sql(
        "SELECT department, COUNT(*), SUM(salary), AVG(salary), MIN(salary), MAX(salary) FROM employees GROUP BY department;",
        IsolationLevel::ReadCommitted,
        &mut writer4,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_aggregation_with_null_values() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    db.execute_sql(
        "CREATE TABLE test_nulls (id INTEGER, value INTEGER, category VARCHAR(10));",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data with NULLs
    db.execute_sql(
        "INSERT INTO test_nulls VALUES 
         (1, 10, 'A'),
         (2, NULL, 'A'),
         (3, 20, 'B'),
         (4, 30, 'A'),
         (5, NULL, 'B');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test COUNT(*) with NULLs - should count all rows
    let mut writer1 = CliResultWriter::new();
    db.execute_sql(
        "SELECT COUNT(*) FROM test_nulls;",
        IsolationLevel::ReadCommitted,
        &mut writer1,
    )
    .await
    .unwrap();

    // Test COUNT(column) with NULLs - should exclude NULLs
    let mut writer2 = CliResultWriter::new();
    db.execute_sql(
        "SELECT COUNT(value) FROM test_nulls;",
        IsolationLevel::ReadCommitted,
        &mut writer2,
    )
    .await
    .unwrap();

    // Test SUM with NULLs - should ignore NULLs
    let mut writer3 = CliResultWriter::new();
    db.execute_sql(
        "SELECT SUM(value) FROM test_nulls;",
        IsolationLevel::ReadCommitted,
        &mut writer3,
    )
    .await
    .unwrap();

    // Test AVG with NULLs - should ignore NULLs
    let mut writer4 = CliResultWriter::new();
    db.execute_sql(
        "SELECT AVG(value) FROM test_nulls;",
        IsolationLevel::ReadCommitted,
        &mut writer4,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_aggregation_on_empty_table() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create empty test table
    db.execute_sql(
        "CREATE TABLE empty_table (id INTEGER, value INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test COUNT(*) on empty table
    let mut writer1 = CliResultWriter::new();
    db.execute_sql(
        "SELECT COUNT(*) FROM empty_table;",
        IsolationLevel::ReadCommitted,
        &mut writer1,
    )
    .await
    .unwrap();

    // Test SUM on empty table - should return NULL
    let mut writer2 = CliResultWriter::new();
    db.execute_sql(
        "SELECT SUM(value) FROM empty_table;",
        IsolationLevel::ReadCommitted,
        &mut writer2,
    )
    .await
    .unwrap();

    // Test AVG on empty table - should return NULL
    let mut writer3 = CliResultWriter::new();
    db.execute_sql(
        "SELECT AVG(value) FROM empty_table;",
        IsolationLevel::ReadCommitted,
        &mut writer3,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_complex_aggregation_scenarios() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    db.execute_sql(
        "CREATE TABLE sales (id INTEGER, product VARCHAR(20), category VARCHAR(20), price INTEGER, quantity INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO sales VALUES 
         (1, 'Laptop', 'Electronics', 1000, 2),
         (2, 'Mouse', 'Electronics', 25, 10),
         (3, 'Chair', 'Furniture', 200, 5),
         (4, 'Desk', 'Furniture', 500, 3),
         (5, 'Phone', 'Electronics', 800, 1);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test aggregation with calculated fields
    let mut writer1 = CliResultWriter::new();
    db.execute_sql(
        "SELECT category, SUM(price * quantity) as total_revenue FROM sales GROUP BY category;",
        IsolationLevel::ReadCommitted,
        &mut writer1,
    )
    .await
    .unwrap();

    // Test multiple levels of aggregation
    let mut writer2 = CliResultWriter::new();
    db.execute_sql(
        "SELECT category, COUNT(*) as product_count, AVG(price) as avg_price, SUM(quantity) as total_quantity FROM sales GROUP BY category ORDER BY category;",
        IsolationLevel::ReadCommitted,
        &mut writer2,
    )
    .await
    .unwrap();

    // Test aggregation with WHERE and HAVING
    let mut writer3 = CliResultWriter::new();
    db.execute_sql(
        "SELECT category, COUNT(*) FROM sales WHERE price > 100 GROUP BY category HAVING COUNT(*) > 1;",
        IsolationLevel::ReadCommitted,
        &mut writer3,
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn test_aggregation_performance() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create test table
    db.execute_sql(
        "CREATE TABLE large_dataset (id INTEGER, group_id INTEGER, value INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert larger dataset for performance testing
    // Note: Using smaller dataset than original for CI performance
    for batch_start in (1..=100).step_by(10) {
        let mut values = Vec::new();
        for i in batch_start..=(batch_start + 9).min(100) {
            values.push(format!("({}, {}, {})", i, i % 10, i * 2));
        }
        let sql = format!("INSERT INTO large_dataset VALUES {};", values.join(", "));
        
        db.execute_sql(
            &sql,
            IsolationLevel::ReadCommitted,
            &mut writer,
        )
        .await
        .unwrap();
    }

    // Test aggregation performance queries
    let test_cases = vec![
        "SELECT COUNT(*) FROM large_dataset;",
        "SELECT SUM(value) FROM large_dataset;",
        "SELECT AVG(value) FROM large_dataset;",
        "SELECT MIN(value), MAX(value) FROM large_dataset;",
        "SELECT group_id, COUNT(*) FROM large_dataset GROUP BY group_id;",
        "SELECT group_id, SUM(value), AVG(value) FROM large_dataset GROUP BY group_id;",
    ];

    for sql in test_cases {
        let mut test_writer = CliResultWriter::new();
        db.execute_sql(sql, IsolationLevel::ReadCommitted, &mut test_writer)
            .await
            .unwrap();
    }
}

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