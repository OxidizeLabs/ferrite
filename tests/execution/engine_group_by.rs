#![allow(clippy::useless_vec)]

use ferrite::catalog::column::Column;
use ferrite::catalog::schema::Schema;
use ferrite::common::result_writer::ResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;
use ferrite::types_db::type_id::TypeId;
use ferrite::types_db::value::Value;

use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;

/// Test-specific result writer that allows inspection of results
struct TestResultWriter {
    schema: Option<Schema>,
    rows: Vec<Vec<Value>>,
}

impl TestResultWriter {
    fn new() -> Self {
        Self {
            schema: None,
            rows: Vec::new(),
        }
    }

    fn get_schema(&self) -> &Schema {
        self.schema.as_ref().expect("Schema not set")
    }

    fn get_rows(&self) -> &Vec<Vec<Value>> {
        &self.rows
    }
}

impl ResultWriter for TestResultWriter {
    fn write_schema_header(&mut self, columns: Vec<String>) {
        let schema_columns = columns
            .into_iter()
            .map(|name| Column::new(&name, TypeId::VarChar))
            .collect();
        self.schema = Some(Schema::new(schema_columns));
    }

    fn write_row(&mut self, values: Vec<Value>) {
        self.rows.push(values);
    }

    fn write_row_with_schema(&mut self, values: Vec<Value>, schema: &Schema) {
        // Store the schema for potential future use
        self.schema = Some(schema.clone());
        self.rows.push(values);
    }

    fn write_message(&mut self, _message: &str) {
        // Ignore messages for testing
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// #[ignore]
async fn test_group_by_column_names() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE employees (name VARCHAR(50), age INTEGER, salary BIGINT);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO employees VALUES
         ('Alice', 25, 50000),
         ('Alice', 25, 52000),
         ('Bob', 30, 60000),
         ('Bob', 30, 65000),
         ('Charlie', 35, 70000);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test different GROUP BY queries
    let test_cases = vec![
        (
            "SELECT name, SUM(age) as total_age, COUNT(*) as emp_count FROM employees GROUP BY name",
            vec!["name", "total_age", "emp_count"],
            3, // Expected number of groups
        ),
        (
            "SELECT name, AVG(salary) as avg_salary FROM employees GROUP BY name",
            vec!["name", "avg_salary"],
            3,
        ),
        (
            "SELECT name, MIN(age) as min_age, MAX(salary) as max_salary FROM employees GROUP BY name",
            vec!["name", "min_age", "max_salary"],
            3,
        ),
    ];

    for (sql, expected_columns, expected_groups) in test_cases {
        let mut writer = TestResultWriter::new();
        db.execute_sql(sql, IsolationLevel::ReadCommitted, &mut writer)
            .await
            .unwrap();

        // Check column names
        let schema = writer.get_schema();
        for (i, expected_name) in expected_columns.iter().enumerate() {
            assert_eq!(
                schema.get_columns()[i].get_name(),
                *expected_name,
                "Column name mismatch for query: {}",
                sql
            );
        }

        // Check number of result groups
        assert_eq!(
            writer.get_rows().len(),
            expected_groups,
            "Incorrect number of groups for query: {}",
            sql
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_aggregates() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE employees (name VARCHAR(50), age INTEGER, salary BIGINT, department VARCHAR(50));",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO employees VALUES
         ('Alice', 25, 50000, 'Engineering'),
         ('Alice', 25, 52000, 'Engineering'),
         ('Bob', 30, 60000, 'Sales'),
         ('Bob', 30, 65000, 'Sales'),
         ('Charlie', 35, 70000, 'Engineering'),
         ('David', 40, 80000, 'Sales');",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    let test_cases = vec![
        // Basic GROUP BY with multiple aggregates
        (
            "SELECT name, COUNT(*) as count, SUM(salary) as total_salary FROM employees GROUP BY name",
            vec!["name", "count", "total_salary"],
            4, // Alice, Bob, Charlie, David
        ),
        // GROUP BY with AVG
        (
            "SELECT department, AVG(salary) as avg_salary FROM employees GROUP BY department",
            vec!["department", "avg_salary"],
            2, // Engineering, Sales
        ),
        // GROUP BY with MIN/MAX
        (
            "SELECT department, MIN(age) as min_age, MAX(salary) as max_salary FROM employees GROUP BY department",
            vec!["department", "min_age", "max_salary"],
            2,
        ),
        // Multiple GROUP BY columns
        (
            "SELECT department, age, COUNT(*) as count FROM employees GROUP BY department, age",
            vec!["department", "age", "count"],
            4, // Unique department-age combinations: Engineering-25, Engineering-35, Sales-30, Sales-40
        ),
    ];

    for (sql, expected_columns, expected_groups) in test_cases {
        let mut writer = TestResultWriter::new();
        db.execute_sql(sql, IsolationLevel::ReadCommitted, &mut writer)
            .await
            .unwrap();

        // Verify column names
        let schema = writer.get_schema();
        assert_eq!(
            schema.get_columns().len(),
            expected_columns.len(),
            "Incorrect number of columns for query: {}",
            sql
        );

        for (i, expected_name) in expected_columns.iter().enumerate() {
            assert_eq!(
                schema.get_columns()[i].get_name(),
                *expected_name,
                "Column name mismatch for query: {}",
                sql
            );
        }

        // Verify number of result rows
        assert_eq!(
            writer.get_rows().len(),
            expected_groups,
            "Incorrect number of groups for query: {}",
            sql
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_single_column() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE products (category VARCHAR(50), price INTEGER, quantity INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO products VALUES
         ('Electronics', 100, 5),
         ('Electronics', 150, 3),
         ('Clothing', 50, 10),
         ('Clothing', 75, 8),
         ('Books', 25, 15),
         ('Books', 30, 12);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test basic GROUP BY with single column
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT category, COUNT(*) as item_count FROM products GROUP BY category",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(writer.get_rows().len(), 3, "Should return 3 categories");

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 2, "Should have 2 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "category");
    assert_eq!(schema.get_columns()[1].get_name(), "item_count");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_multiple_columns() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE departments (department VARCHAR(50), location VARCHAR(50), employee_count INTEGER, budget BIGINT);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO departments VALUES
         ('Engineering', 'NYC', 10, 100000),
         ('Engineering', 'SF', 15, 150000),
         ('Sales', 'NYC', 8, 80000),
         ('Sales', 'SF', 12, 120000),
         ('Marketing', 'NYC', 5, 50000);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test GROUP BY with multiple columns
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT department, location, SUM(employee_count) as total_employees, AVG(budget) as avg_budget FROM departments GROUP BY department, location",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(
        writer.get_rows().len(),
        5,
        "Should return 5 unique department-location combinations"
    );

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 4, "Should have 4 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "department");
    assert_eq!(schema.get_columns()[1].get_name(), "location");
    assert_eq!(schema.get_columns()[2].get_name(), "total_employees");
    assert_eq!(schema.get_columns()[3].get_name(), "avg_budget");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_with_where_clause() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE inventory (product_name VARCHAR(50), category VARCHAR(50), price INTEGER, in_stock BOOLEAN);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO inventory VALUES
         ('Laptop', 'Electronics', 1000, true),
         ('Phone', 'Electronics', 500, true),
         ('Tablet', 'Electronics', 300, false),
         ('Shirt', 'Clothing', 50, true),
         ('Pants', 'Clothing', 75, false),
         ('Novel', 'Books', 20, true);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test GROUP BY with WHERE clause filtering
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT category, COUNT(*) as available_items, AVG(price) as avg_price FROM inventory WHERE in_stock = true GROUP BY category",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(
        writer.get_rows().len(),
        3,
        "Should return 3 categories with available items"
    );

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 3, "Should have 3 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "category");
    assert_eq!(schema.get_columns()[1].get_name(), "available_items");
    assert_eq!(schema.get_columns()[2].get_name(), "avg_price");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_with_having_clause() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE players (team VARCHAR(50), player_name VARCHAR(50), score INTEGER, games_played INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO players VALUES
         ('Team A', 'Alice', 100, 5),
         ('Team A', 'Bob', 150, 5),
         ('Team A', 'Charlie', 80, 5),
         ('Team B', 'David', 200, 4),
         ('Team B', 'Eve', 120, 4),
         ('Team C', 'Frank', 90, 3);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test GROUP BY with HAVING clause
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT team, COUNT(*) as player_count, AVG(score) as avg_score FROM players GROUP BY team HAVING COUNT(*) > 2",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(
        writer.get_rows().len(),
        1,
        "Should return 1 team with more than 2 players"
    );

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 3, "Should have 3 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "team");
    assert_eq!(schema.get_columns()[1].get_name(), "player_count");
    assert_eq!(schema.get_columns()[2].get_name(), "avg_score");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_with_order_by() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE sales (region VARCHAR(50), sales_amount INTEGER, quarter INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO sales VALUES
         ('North', 1000, 1),
         ('North', 1200, 2),
         ('South', 800, 1),
         ('South', 900, 2),
         ('East', 1500, 1),
         ('West', 700, 1);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test GROUP BY with ORDER BY
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT region, SUM(sales_amount) as total_sales FROM sales GROUP BY region ORDER BY total_sales DESC",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(writer.get_rows().len(), 4, "Should return 4 regions");

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 2, "Should have 2 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "region");
    assert_eq!(schema.get_columns()[1].get_name(), "total_sales");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_with_null_values() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE test_nulls (category VARCHAR(50), value INTEGER, description VARCHAR(50));",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data with NULL values
    db.execute_sql(
        "INSERT INTO test_nulls VALUES
         ('A', 10, 'Description A'),
         ('A', NULL, 'Description A2'),
         (NULL, 20, 'Description B'),
         (NULL, 30, 'Description B2'),
         ('B', 40, NULL);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test GROUP BY with NULL handling
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT category, COUNT(*) as row_count, COUNT(value) as non_null_values FROM test_nulls GROUP BY category",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(
        writer.get_rows().len(),
        3,
        "Should return 3 groups (A, B, NULL)"
    );

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 3, "Should have 3 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "category");
    assert_eq!(schema.get_columns()[1].get_name(), "row_count");
    assert_eq!(schema.get_columns()[2].get_name(), "non_null_values");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_all_aggregation_functions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE measurements (group_id VARCHAR(50), value INTEGER, weight INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data
    db.execute_sql(
        "INSERT INTO measurements VALUES
         ('Group1', 10, 1),
         ('Group1', 20, 2),
         ('Group1', 30, 3),
         ('Group2', 5, 1),
         ('Group2', 15, 4),
         ('Group2', 25, 2);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test all aggregation functions with GROUP BY
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT group_id, COUNT(*) as count, SUM(value) as sum_val, AVG(value) as avg_val, MIN(value) as min_val, MAX(value) as max_val FROM measurements GROUP BY group_id",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(writer.get_rows().len(), 2, "Should return 2 groups");

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 6, "Should have 6 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "group_id");
    assert_eq!(schema.get_columns()[1].get_name(), "count");
    assert_eq!(schema.get_columns()[2].get_name(), "sum_val");
    assert_eq!(schema.get_columns()[3].get_name(), "avg_val");
    assert_eq!(schema.get_columns()[4].get_name(), "min_val");
    assert_eq!(schema.get_columns()[5].get_name(), "max_val");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_empty_table() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE empty_table (category VARCHAR(50), value INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // No data inserted - table is empty

    // Test GROUP BY on empty table
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT category, COUNT(*) as count FROM empty_table GROUP BY category",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(
        writer.get_rows().len(),
        0,
        "Should return 0 rows for empty table"
    );

    // Verify schema is still correct
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 2, "Should have 2 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "category");
    assert_eq!(schema.get_columns()[1].get_name(), "count");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 5)]
async fn test_group_by_performance_large_dataset() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE large_dataset (category VARCHAR(50), subcategory VARCHAR(50), value INTEGER, timestamp BIGINT);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert large dataset
    let mut insert_statements = Vec::new();
    let categories = vec!["A", "B", "C", "D", "E"];
    let subcategories = vec!["X", "Y", "Z"];

    for i in 0..1000 {
        let category = categories[i % categories.len()];
        let subcategory = subcategories[i % subcategories.len()];
        let value = (i % 100) as i32;
        let timestamp = i as i64;

        insert_statements.push(format!(
            "('{}', '{}', {}, {})",
            category, subcategory, value, timestamp
        ));
    }

    // Insert in batches to avoid too large SQL statements
    for chunk in insert_statements.chunks(200) {
        let values = chunk.join(", ");
        let sql = format!("INSERT INTO large_dataset VALUES {}", values);

        db.execute_sql(
            &sql,
            IsolationLevel::ReadCommitted,
            &mut TestResultWriter::new(),
        )
        .await
        .unwrap();
    }

    // Test GROUP BY performance with large dataset
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT category, subcategory, COUNT(*) as record_count, AVG(value) as avg_value, SUM(value) as total_value FROM large_dataset GROUP BY category, subcategory",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(
        writer.get_rows().len(),
        15,
        "Should return 15 groups (5 categories × 3 subcategories)"
    );

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 5, "Should have 5 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "category");
    assert_eq!(schema.get_columns()[1].get_name(), "subcategory");
    assert_eq!(schema.get_columns()[2].get_name(), "record_count");
    assert_eq!(schema.get_columns()[3].get_name(), "avg_value");
    assert_eq!(schema.get_columns()[4].get_name(), "total_value");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_group_by_distinct_operations() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();

    // Create test table
    db.execute_sql(
        "CREATE TABLE employee_skills (department VARCHAR(50), skill VARCHAR(50), employee_name VARCHAR(50), rating INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Insert test data with some duplicates
    db.execute_sql(
        "INSERT INTO employee_skills VALUES
         ('Engineering', 'Python', 'Alice', 9),
         ('Engineering', 'Python', 'Bob', 8),
         ('Engineering', 'Java', 'Alice', 7),
         ('Engineering', 'Java', 'Charlie', 9),
         ('Marketing', 'Python', 'David', 6),
         ('Marketing', 'Design', 'Eve', 8),
         ('Marketing', 'Design', 'Frank', 7);",
        IsolationLevel::ReadCommitted,
        &mut TestResultWriter::new(),
    )
    .await
    .unwrap();

    // Test GROUP BY with COUNT(DISTINCT)
    let mut writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT department, COUNT(DISTINCT skill) as unique_skills, COUNT(*) as total_records FROM employee_skills GROUP BY department",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    assert_eq!(writer.get_rows().len(), 2, "Should return 2 departments");

    // Verify schema
    let schema = writer.get_schema();
    assert_eq!(schema.get_columns().len(), 3, "Should have 3 columns");
    assert_eq!(schema.get_columns()[0].get_name(), "department");
    assert_eq!(schema.get_columns()[1].get_name(), "unique_skills");
    assert_eq!(schema.get_columns()[2].get_name(), "total_records");
}
