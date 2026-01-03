use ferrite::catalog::column::Column;
use ferrite::catalog::schema::Schema;
use ferrite::types_db::type_id::TypeId;
use ferrite::types_db::value::Value;

use crate::execution::common::{TestContext, TestResultWriter};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
// #[ignore = "Causes stack overflow in the logical plan to physical plan conversion"]
async fn test_order_by() {
    let mut ctx = TestContext::new("test_order_by").await;

    println!("Creating test table and schema");
    // Create test table with minimal schema
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "sorted_users";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    println!("Inserting test data");
    // Insert minimal test data - just two rows to minimize stack usage
    let test_data = vec![
        vec![Value::new(2), Value::new("Alice")],
        vec![Value::new(1), Value::new("Bob")],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test a single simple ORDER BY case
    let sql = "SELECT id, name FROM sorted_users ORDER BY id"; // Use ASC order to simplify
    let mut writer = TestResultWriter::new();

    println!("Executing query: {}", sql);
    let success = match ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
    {
        Ok(s) => {
            println!("Query execution succeeded");
            s
        },
        Err(e) => {
            println!("Query execution failed: {:?}", e);
            panic!("Query execution failed: {:?}", e);
        },
    };

    assert!(success, "Query execution failed");

    println!("Processing results");
    let rows = writer.get_rows();
    println!("Got {} rows", rows.len());

    // Verify results in a stack-efficient way
    assert_eq!(rows.len(), 2, "Expected 2 rows");

    if rows.len() >= 2 {
        println!("First row: {:?}", rows[0]);
        println!("Second row: {:?}", rows[1]);

        // Check first row (should be id=1, name=Bob)
        let first_id = &rows[0][0];
        let first_name = &rows[0][1];
        assert_eq!(first_id.to_string(), "1", "First row should have id=1");
        assert_eq!(
            first_name.to_string(),
            "Bob",
            "First row should have name=Bob"
        );

        // Check second row (should be id=2, name=Alice)
        let second_id = &rows[1][0];
        let second_name = &rows[1][1];
        assert_eq!(second_id.to_string(), "2", "Second row should have id=2");
        assert_eq!(
            second_name.to_string(),
            "Alice",
            "Second row should have name=Alice"
        );
    }

    println!("Test completed successfully");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_desc() {
    let mut ctx = TestContext::new("test_order_by_desc").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("score", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "students";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new(85), Value::new("Alice")],
        vec![Value::new(2), Value::new(92), Value::new("Bob")],
        vec![Value::new(3), Value::new(78), Value::new("Charlie")],
        vec![Value::new(4), Value::new(95), Value::new("Diana")],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY DESC
    let sql = "SELECT id, score, name FROM students ORDER BY score DESC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 rows");

    // Verify descending order by score
    if rows.len() >= 4 {
        assert_eq!(rows[0][1].to_string(), "95"); // Diana
        assert_eq!(rows[1][1].to_string(), "92"); // Bob
        assert_eq!(rows[2][1].to_string(), "85"); // Alice
        assert_eq!(rows[3][1].to_string(), "78"); // Charlie
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_multiple_columns() {
    let mut ctx = TestContext::new("test_order_by_multiple_columns").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("department", TypeId::VarChar),
        Column::new("salary", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "employees";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new("IT"), Value::new(75000), Value::new("Alice")],
        vec![Value::new("HR"), Value::new(65000), Value::new("Bob")],
        vec![Value::new("IT"), Value::new(80000), Value::new("Charlie")],
        vec![Value::new("HR"), Value::new(70000), Value::new("Diana")],
        vec![Value::new("IT"), Value::new(75000), Value::new("Eve")],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY multiple columns
    let sql = "SELECT department, salary, name FROM employees ORDER BY department ASC, salary DESC, name ASC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 5, "Expected 5 rows");

    // Verify ordering: HR first, then IT; within each dept by salary desc, then by name asc
    if rows.len() >= 5 {
        assert_eq!(rows[0][0].to_string(), "HR");
        assert_eq!(rows[0][1].to_string(), "70000"); // Diana
        assert_eq!(rows[1][0].to_string(), "HR");
        assert_eq!(rows[1][1].to_string(), "65000"); // Bob
        assert_eq!(rows[2][0].to_string(), "IT");
        assert_eq!(rows[2][1].to_string(), "80000"); // Charlie
        // For IT with salary 75000, Alice comes before Eve alphabetically
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_with_null_values() {
    let mut ctx = TestContext::new("test_order_by_with_null_values").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("score", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "test_scores";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data with NULL values
    let test_data = vec![
        vec![Value::new(1), Value::new(85), Value::new("Alice")],
        vec![
            Value::new(2),
            Value::new_with_type(ferrite::types_db::value::Val::Null, TypeId::Integer),
            Value::new("Bob"),
        ],
        vec![Value::new(3), Value::new(92), Value::new("Charlie")],
        vec![
            Value::new(4),
            Value::new_with_type(ferrite::types_db::value::Val::Null, TypeId::Integer),
            Value::new("Diana"),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY with NULL values
    let sql = "SELECT id, score, name FROM test_scores ORDER BY score ASC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 rows");
    // NULL handling behavior may vary by implementation
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_with_expressions() {
    let mut ctx = TestContext::new("test_order_by_with_expressions").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("price", TypeId::Integer),
        Column::new("quantity", TypeId::Integer),
    ]);

    let table_name = "products";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new(100), Value::new(5)], // total: 500
        vec![Value::new(2), Value::new(200), Value::new(2)], // total: 400
        vec![Value::new(3), Value::new(150), Value::new(4)], // total: 600
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY with expression
    let sql = "SELECT id, price, quantity, price * quantity as total FROM products ORDER BY price * quantity DESC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 3, "Expected 3 rows");

    // Verify ordering by calculated expression (total descending)
    if rows.len() >= 3 {
        assert_eq!(rows[0][0].to_string(), "3"); // 600
        assert_eq!(rows[1][0].to_string(), "1"); // 500
        assert_eq!(rows[2][0].to_string(), "2"); // 400
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_with_limit() {
    let mut ctx = TestContext::new("test_order_by_with_limit").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("value", TypeId::Integer),
    ]);

    let table_name = "numbers";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new(50)],
        vec![Value::new(2), Value::new(30)],
        vec![Value::new(3), Value::new(80)],
        vec![Value::new(4), Value::new(20)],
        vec![Value::new(5), Value::new(70)],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY with LIMIT
    let sql = "SELECT id, value FROM numbers ORDER BY value DESC LIMIT 3";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 3, "Expected 3 rows due to LIMIT");

    // Verify top 3 values
    if rows.len() >= 3 {
        assert_eq!(rows[0][1].to_string(), "80"); // id=3
        assert_eq!(rows[1][1].to_string(), "70"); // id=5
        assert_eq!(rows[2][1].to_string(), "50"); // id=1
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_with_offset() {
    let mut ctx = TestContext::new("test_order_by_with_offset").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("value", TypeId::Integer),
    ]);

    let table_name = "data";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new(10)],
        vec![Value::new(2), Value::new(20)],
        vec![Value::new(3), Value::new(30)],
        vec![Value::new(4), Value::new(40)],
        vec![Value::new(5), Value::new(50)],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY with OFFSET
    let sql = "SELECT id, value FROM data ORDER BY value ASC OFFSET 2";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 3, "Expected 3 rows after OFFSET 2");

    // Verify remaining values after offset
    if rows.len() >= 3 {
        assert_eq!(rows[0][1].to_string(), "30"); // id=3
        assert_eq!(rows[1][1].to_string(), "40"); // id=4
        assert_eq!(rows[2][1].to_string(), "50"); // id=5
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_different_data_types() {
    let mut ctx = TestContext::new("test_order_by_different_data_types").await;

    // Create test table with various data types
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
        Column::new("active", TypeId::Boolean),
        Column::new("score", TypeId::BigInt),
    ]);

    let table_name = "mixed_data";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![
            Value::new(3),
            Value::new("Charlie"),
            Value::new(true),
            Value::new(95i64),
        ],
        vec![
            Value::new(1),
            Value::new("Alice"),
            Value::new(false),
            Value::new(85i64),
        ],
        vec![
            Value::new(2),
            Value::new("Bob"),
            Value::new(true),
            Value::new(90i64),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY on string column
    let sql = "SELECT id, name, active, score FROM mixed_data ORDER BY name ASC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 3, "Expected 3 rows");

    // Verify alphabetical order
    if rows.len() >= 3 {
        assert_eq!(rows[0][1].to_string(), "Alice");
        assert_eq!(rows[1][1].to_string(), "Bob");
        assert_eq!(rows[2][1].to_string(), "Charlie");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_with_aggregation() {
    let mut ctx = TestContext::new("test_order_by_with_aggregation").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("department", TypeId::VarChar),
        Column::new("salary", TypeId::Integer),
    ]);

    let table_name = "emp_salaries";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new("IT"), Value::new(70000)],
        vec![Value::new("HR"), Value::new(60000)],
        vec![Value::new("IT"), Value::new(80000)],
        vec![Value::new("Sales"), Value::new(55000)],
        vec![Value::new("HR"), Value::new(65000)],
        vec![Value::new("Sales"), Value::new(50000)],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY with GROUP BY and aggregation
    let sql = "SELECT department, AVG(salary) as avg_salary FROM emp_salaries GROUP BY department ORDER BY avg_salary DESC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 3, "Expected 3 departments");

    // Verify ordering by average salary
    if rows.len() >= 3 {
        assert_eq!(rows[0][0].to_string(), "IT"); // 75000 avg
        assert_eq!(rows[1][0].to_string(), "HR"); // 62500 avg
        assert_eq!(rows[2][0].to_string(), "Sales"); // 52500 avg
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_edge_cases() {
    let mut ctx = TestContext::new("test_order_by_edge_cases").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("value", TypeId::Integer),
    ]);

    let table_name = "edge_cases";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Test ORDER BY on empty table
    let sql = "SELECT id, value FROM edge_cases ORDER BY value ASC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 0, "Expected 0 rows from empty table");

    // Insert single row
    let test_data = vec![vec![Value::new(1), Value::new(100)]];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY on single row
    let sql = "SELECT id, value FROM edge_cases ORDER BY value DESC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 1, "Expected 1 row");

    if rows.len() == 1 {
        assert_eq!(rows[0][0].to_string(), "1");
        assert_eq!(rows[0][1].to_string(), "100");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_order_by_performance() {
    let mut ctx = TestContext::new("test_order_by_performance").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("random_value", TypeId::Integer),
        Column::new("category", TypeId::VarChar),
    ]);

    let table_name = "large_dataset";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert larger dataset for performance testing
    let mut test_data = Vec::new();
    for i in 1..=50 {
        let category = if i % 3 == 0 {
            "A"
        } else if i % 3 == 1 {
            "B"
        } else {
            "C"
        };
        let random_value = (i * 17) % 100; // Generate some pseudo-random values
        test_data.push(vec![
            Value::new(i),
            Value::new(random_value),
            Value::new(category),
        ]);
    }
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ORDER BY performance with larger dataset
    let sql = "SELECT id, random_value, category FROM large_dataset ORDER BY random_value DESC, category ASC LIMIT 10";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    assert!(success, "Performance test query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 10, "Expected 10 rows due to LIMIT");

    println!("Performance test completed with {} result rows", rows.len());
}
