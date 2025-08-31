use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use tkdb::catalog::column::Column;
use tkdb::catalog::schema::Schema;
use tkdb::common::result_writer::CliResultWriter;
use tkdb::concurrency::transaction::IsolationLevel;
use tkdb::types_db::type_id::TypeId;
use tkdb::types_db::value::Val::Null;
use tkdb::types_db::value::Value;

/// Test suite for join operations covering all join types and edge cases
/// Migrated from src/sql/execution/execution_engine.rs join_tests module

#[tokio::test]
async fn test_join_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create users table
    db.execute_sql(
        "CREATE TABLE users (id INTEGER, name VARCHAR(50), dept_id INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Create departments table
    db.execute_sql(
        "CREATE TABLE departments (id INTEGER, name VARCHAR(50))",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data for users
    db.execute_sql(
        "INSERT INTO users VALUES (1, 'Alice', 1), (2, 'Bob', 2), (3, 'Charlie', 1), (4, 'David', 3), (5, 'Eve', NULL)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data for departments
    db.execute_sql(
        "INSERT INTO departments VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'Marketing')",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Test inner join
    let mut inner_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id",
            IsolationLevel::ReadCommitted,
            &mut inner_writer,
        )
        .await
        .unwrap();
    assert!(success, "Inner join query execution failed");

    // Test left outer join
    let mut left_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT u.name, d.name FROM users u LEFT OUTER JOIN departments d ON u.dept_id = d.id",
            IsolationLevel::ReadCommitted,
            &mut left_writer,
        )
        .await
        .unwrap();
    assert!(success, "Left outer join query execution failed");

    // Test left join (alternative syntax)
    let mut left_alt_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT u.name, d.name FROM users u LEFT JOIN departments d ON u.dept_id = d.id",
            IsolationLevel::ReadCommitted,
            &mut left_alt_writer,
        )
        .await
        .unwrap();
    assert!(success, "Left join query execution failed");

    // Test join with additional WHERE conditions
    let mut where_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT u.name, d.name FROM users u JOIN departments d ON u.dept_id = d.id WHERE d.name = 'Engineering'",
            IsolationLevel::ReadCommitted,
            &mut where_writer,
        )
        .await
        .unwrap();
    assert!(success, "Join with WHERE condition query execution failed");
}

#[tokio::test]
async fn test_right_join_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create employees table
    db.execute_sql(
        "CREATE TABLE employees (id INTEGER, name VARCHAR(50), dept_id INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Create departments table with budget
    db.execute_sql(
        "CREATE TABLE departments (id INTEGER, name VARCHAR(50), budget INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data - some departments without employees
    db.execute_sql(
        "INSERT INTO employees VALUES (1, 'Alice', 1), (2, 'Bob', 2)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO departments VALUES (1, 'Engineering', 100000), (2, 'Sales', 50000), (3, 'Marketing', 75000), (4, 'HR', 30000)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Right join - should return all departments, even those without employees
    let mut right_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT e.name, d.name FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id",
            IsolationLevel::ReadCommitted,
            &mut right_writer,
        )
        .await
        .unwrap();
    assert!(success, "Right join query execution failed");

    // Right join with ordering to ensure consistent results
    let mut ordered_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT e.name, d.name FROM employees e RIGHT JOIN departments d ON e.dept_id = d.id ORDER BY d.id",
            IsolationLevel::ReadCommitted,
            &mut ordered_writer,
        )
        .await
        .unwrap();
    assert!(success, "Right join with ORDER BY failed");
}

#[tokio::test]
async fn test_full_outer_join_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create customers table
    db.execute_sql(
        "CREATE TABLE customers (id INTEGER, name VARCHAR(50), city_id INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Create cities table
    db.execute_sql(
        "CREATE TABLE cities (id INTEGER, name VARCHAR(50))",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert customers - some in cities not in cities table
    db.execute_sql(
        "INSERT INTO customers VALUES (1, 'John', 1), (2, 'Jane', 2), (3, 'Bob', 99)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert cities - some without customers
    db.execute_sql(
        "INSERT INTO cities VALUES (1, 'New York'), (2, 'Los Angeles'), (3, 'Chicago')",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Full outer join - should return all combinations
    let mut full_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT c.name, ci.name FROM customers c FULL OUTER JOIN cities ci ON c.city_id = ci.id",
            IsolationLevel::ReadCommitted,
            &mut full_writer,
        )
        .await
        .unwrap();
    assert!(success, "Full outer join query execution failed");
}

#[tokio::test]
#[ignore] // Marked as ignore in original implementation
async fn test_cross_join_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create small tables for cross join
    db.execute_sql(
        "CREATE TABLE colors (id INTEGER, color VARCHAR(50))",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE TABLE sizes (id INTEGER, size VARCHAR(50))",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO colors VALUES (1, 'Red'), (2, 'Blue')",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO sizes VALUES (1, 'Small'), (2, 'Large')",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Cross join
    let mut cross_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT c.color, s.size FROM colors c CROSS JOIN sizes s",
            IsolationLevel::ReadCommitted,
            &mut cross_writer,
        )
        .await
        .unwrap();
    assert!(success, "Cross join query execution failed");

    // Alternative syntax for cross join
    let mut implicit_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT c.color, s.size FROM colors c, sizes s",
            IsolationLevel::ReadCommitted,
            &mut implicit_writer,
        )
        .await
        .unwrap();
    assert!(success, "Implicit cross join query execution failed");
}

#[tokio::test]
async fn test_self_join_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create employees table with manager relationship
    db.execute_sql(
        "CREATE TABLE employees (id INTEGER, name VARCHAR(50), manager_id INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO employees VALUES (1, 'CEO', NULL), (2, 'Manager1', 1), (3, 'Manager2', 1), (4, 'Employee1', 2), (5, 'Employee2', 2), (6, 'Employee3', 3)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Self join to find employee-manager relationships
    let mut self_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT e.name AS employee, m.name AS manager FROM employees e LEFT JOIN employees m ON e.manager_id = m.id",
            IsolationLevel::ReadCommitted,
            &mut self_writer,
        )
        .await
        .unwrap();
    assert!(success, "Self join query execution failed");

    // Self join to find colleagues (employees with same manager)
    let mut colleagues_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT e1.name, e2.name FROM employees e1 JOIN employees e2 ON e1.manager_id = e2.manager_id AND e1.id != e2.id",
            IsolationLevel::ReadCommitted,
            &mut colleagues_writer,
        )
        .await
        .unwrap();
    assert!(success, "Colleagues self join query execution failed");
}

#[tokio::test]
async fn test_multiple_table_joins() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create multiple related tables
    db.execute_sql(
        "CREATE TABLE customers (id INTEGER, name VARCHAR(50))",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE TABLE orders (id INTEGER, customer_id INTEGER, product_id INTEGER, quantity INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE TABLE products (id INTEGER, name VARCHAR(50), price INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO orders VALUES (1, 1, 1, 2), (2, 1, 2, 1), (3, 2, 1, 1)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO products VALUES (1, 'Laptop', 1000), (2, 'Mouse', 25)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Three-table join
    let mut three_table_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT c.name, p.name, o.quantity FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id",
            IsolationLevel::ReadCommitted,
            &mut three_table_writer,
        )
        .await
        .unwrap();
    assert!(success, "Three-table join query execution failed");

    // Three-table join with aggregation
    let mut agg_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT c.name, COUNT(*) as order_count FROM customers c JOIN orders o ON c.id = o.customer_id JOIN products p ON o.product_id = p.id GROUP BY c.name",
            IsolationLevel::ReadCommitted,
            &mut agg_writer,
        )
        .await
        .unwrap();
    assert!(success, "Three-table join with aggregation failed");
}

#[tokio::test]
async fn test_join_with_complex_conditions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create tables for complex join conditions
    db.execute_sql(
        "CREATE TABLE sales (id INTEGER, salesperson_id INTEGER, amount INTEGER, region VARCHAR(50))",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE TABLE salespeople (id INTEGER, name VARCHAR(50), region VARCHAR(50), quota INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO sales VALUES (1, 1, 1000, 'North'), (2, 1, 2000, 'North'), (3, 2, 1500, 'South'), (4, 2, 800, 'North')",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO salespeople VALUES (1, 'Alice', 'North', 2500), (2, 'Bob', 'South', 2000)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Join with multiple conditions
    let mut complex_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT sp.name, s.amount FROM salespeople sp JOIN sales s ON sp.id = s.salesperson_id AND sp.region = s.region",
            IsolationLevel::ReadCommitted,
            &mut complex_writer,
        )
        .await
        .unwrap();
    assert!(success, "Complex join condition query execution failed");

    // Join with comparison in ON clause
    let mut comparison_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT sp.name, s.amount FROM salespeople sp JOIN sales s ON sp.id = s.salesperson_id AND s.amount > 1000",
            IsolationLevel::ReadCommitted,
            &mut comparison_writer,
        )
        .await
        .unwrap();
    assert!(success, "Join with amount comparison failed");
}

#[tokio::test]
async fn test_join_performance_with_large_dataset() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create larger tables to test join performance
    db.execute_sql(
        "CREATE TABLE table_a (id INTEGER, value VARCHAR(50))",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE TABLE table_b (id INTEGER, a_id INTEGER, value VARCHAR(50))",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Insert larger dataset - table_a
    for i in 1..=100 {
        db.execute_sql(
            &format!("INSERT INTO table_a VALUES ({}, 'value_a_{}')", i, i),
            IsolationLevel::ReadCommitted,
            &mut writer,
        )
        .await
        .unwrap();

        // Create multiple B records for each A record
        for j in 1..=3 {
            db.execute_sql(
                &format!(
                    "INSERT INTO table_b VALUES ({}, {}, 'value_b_{}_{}')",
                    i * 10 + j,
                    i,
                    i,
                    j
                ),
                IsolationLevel::ReadCommitted,
                &mut writer,
            )
            .await
            .unwrap();
        }
    }

    // Test join performance
    let mut perf_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT a.value, b.value FROM table_a a JOIN table_b b ON a.id = b.a_id WHERE a.id <= 10",
            IsolationLevel::ReadCommitted,
            &mut perf_writer,
        )
        .await
        .unwrap();
    assert!(success, "Large dataset join query execution failed");
}

#[tokio::test]
async fn test_join_with_null_handling() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = CliResultWriter::new();

    // Create tables with NULL values
    db.execute_sql(
        "CREATE TABLE teachers (id INTEGER, name VARCHAR(50), department_id INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "CREATE TABLE courses (id INTEGER, name VARCHAR(50), teacher_id INTEGER)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO teachers VALUES (1, 'Alice', 1), (2, 'Bob', NULL), (3, 'Charlie', 2)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO courses VALUES (1, 'Math', 1), (2, 'Physics', 2), (3, 'Chemistry', 3), (4, 'Biology', NULL)",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    // Inner join - should exclude NULLs
    let mut inner_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT t.name, c.name FROM teachers t JOIN courses c ON t.id = c.teacher_id",
            IsolationLevel::ReadCommitted,
            &mut inner_writer,
        )
        .await
        .unwrap();
    assert!(success, "Join with NULL values failed");

    // Left join - should include all teachers
    let mut left_writer = CliResultWriter::new();
    let success = db
        .execute_sql(
            "SELECT t.name, c.name FROM teachers t LEFT JOIN courses c ON t.id = c.teacher_id",
            IsolationLevel::ReadCommitted,
            &mut left_writer,
        )
        .await
        .unwrap();
    assert!(success, "Left join with NULL values failed");
}

#[tokio::test] 
async fn basic_inner_join() {
    // Keep the original simple test for compatibility
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