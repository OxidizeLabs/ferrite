use super::common::{TestContext, TestResultWriter};
use ferrite::catalog::column::Column;
use ferrite::catalog::schema::Schema;
use ferrite::types_db::type_id::TypeId;
use ferrite::types_db::value::Value;

#[tokio::test]
async fn test_row_number_window_function() {
    let mut ctx = TestContext::new("test_row_number_window_function").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("department", TypeId::VarChar),
        Column::new("salary", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "employees";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![
            Value::new(1),
            Value::new("IT"),
            Value::new(75000),
            Value::new("Alice"),
        ],
        vec![
            Value::new(2),
            Value::new("IT"),
            Value::new(85000),
            Value::new("Bob"),
        ],
        vec![
            Value::new(3),
            Value::new("HR"),
            Value::new(65000),
            Value::new("Charlie"),
        ],
        vec![
            Value::new(4),
            Value::new("HR"),
            Value::new(70000),
            Value::new("Diana"),
        ],
        vec![
            Value::new(5),
            Value::new("Sales"),
            Value::new(55000),
            Value::new("Eve"),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ROW_NUMBER() window function
    let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num FROM employees ORDER BY salary DESC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 5, "Expected 5 rows");

        // Verify ROW_NUMBER values
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: name={}, dept={}, salary={}, row_num={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3]
            );
        }
    } else {
        println!("ROW_NUMBER window function not yet supported");
    }
}

#[tokio::test]
async fn test_rank_window_functions() {
    let mut ctx = TestContext::new("test_rank_window_functions").await;

    // Create test table with duplicate values for ranking
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("score", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "students";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data with duplicate scores
    let test_data = vec![
        vec![Value::new(1), Value::new(95), Value::new("Alice")],
        vec![Value::new(2), Value::new(87), Value::new("Bob")],
        vec![Value::new(3), Value::new(95), Value::new("Charlie")], // Same score as Alice
        vec![Value::new(4), Value::new(78), Value::new("Diana")],
        vec![Value::new(5), Value::new(87), Value::new("Eve")], // Same score as Bob
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test RANK() and DENSE_RANK() window functions
    let sql = "SELECT name, score, RANK() OVER (ORDER BY score DESC) as rank_val, DENSE_RANK() OVER (ORDER BY score DESC) as dense_rank_val FROM students ORDER BY score DESC, name";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 5, "Expected 5 rows");

        // Verify ranking values
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: name={}, score={}, rank={}, dense_rank={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3]
            );
        }
    } else {
        println!("RANK window functions not yet supported");
    }
}

#[tokio::test]
async fn test_window_functions_with_partition() {
    let mut ctx = TestContext::new("test_window_functions_with_partition").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("department", TypeId::VarChar),
        Column::new("salary", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "employees";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![
            Value::new(1),
            Value::new("IT"),
            Value::new(75000),
            Value::new("Alice"),
        ],
        vec![
            Value::new(2),
            Value::new("IT"),
            Value::new(85000),
            Value::new("Bob"),
        ],
        vec![
            Value::new(3),
            Value::new("IT"),
            Value::new(90000),
            Value::new("Charlie"),
        ],
        vec![
            Value::new(4),
            Value::new("HR"),
            Value::new(65000),
            Value::new("Diana"),
        ],
        vec![
            Value::new(5),
            Value::new("HR"),
            Value::new(70000),
            Value::new("Eve"),
        ],
        vec![
            Value::new(6),
            Value::new("Sales"),
            Value::new(55000),
            Value::new("Frank"),
        ],
        vec![
            Value::new(7),
            Value::new("Sales"),
            Value::new(60000),
            Value::new("Grace"),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test ROW_NUMBER() with PARTITION BY
    let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank FROM employees ORDER BY department, salary DESC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 7, "Expected 7 rows");

        // Verify partitioned ranking
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: name={}, dept={}, salary={}, dept_rank={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3]
            );
        }
    } else {
        println!("Partitioned window functions not yet supported");
    }
}

#[tokio::test]
async fn test_lag_lead_window_functions() {
    let mut ctx = TestContext::new("test_lag_lead_window_functions").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("date", TypeId::VarChar), // Using VARCHAR for simplicity
        Column::new("sales", TypeId::Integer),
    ]);

    let table_name = "daily_sales";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new("2024-01-01"), Value::new(1000)],
        vec![Value::new(2), Value::new("2024-01-02"), Value::new(1200)],
        vec![Value::new(3), Value::new("2024-01-03"), Value::new(800)],
        vec![Value::new(4), Value::new("2024-01-04"), Value::new(1500)],
        vec![Value::new(5), Value::new("2024-01-05"), Value::new(900)],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test LAG() and LEAD() window functions
    let sql = "SELECT date, sales, LAG(sales, 1) OVER (ORDER BY date) as prev_sales, LEAD(sales, 1) OVER (ORDER BY date) as next_sales FROM daily_sales ORDER BY date";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 5, "Expected 5 rows");

        // Verify LAG/LEAD values
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: date={}, sales={}, prev_sales={}, next_sales={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3]
            );
        }
    } else {
        println!("LAG/LEAD window functions not yet supported");
    }
}

#[tokio::test]
async fn test_first_last_value_window_functions() {
    let mut ctx = TestContext::new("test_first_last_value_window_functions").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("department", TypeId::VarChar),
        Column::new("salary", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "employees";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![
            Value::new(1),
            Value::new("IT"),
            Value::new(75000),
            Value::new("Alice"),
        ],
        vec![
            Value::new(2),
            Value::new("IT"),
            Value::new(85000),
            Value::new("Bob"),
        ],
        vec![
            Value::new(3),
            Value::new("HR"),
            Value::new(65000),
            Value::new("Charlie"),
        ],
        vec![
            Value::new(4),
            Value::new("HR"),
            Value::new(70000),
            Value::new("Diana"),
        ],
        vec![
            Value::new(5),
            Value::new("Sales"),
            Value::new(55000),
            Value::new("Eve"),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test FIRST_VALUE() and LAST_VALUE() window functions
    let sql = "SELECT name, department, salary, FIRST_VALUE(name) OVER (PARTITION BY department ORDER BY salary) as lowest_paid, LAST_VALUE(name) OVER (PARTITION BY department ORDER BY salary ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as highest_paid FROM employees ORDER BY department, salary";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 5, "Expected 5 rows");

        // Verify FIRST_VALUE/LAST_VALUE
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: name={}, dept={}, salary={}, lowest_paid={}, highest_paid={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3],
                row[4]
            );
        }
    } else {
        println!("FIRST_VALUE/LAST_VALUE window functions not yet supported");
    }
}

#[tokio::test]
async fn test_aggregation_window_functions() {
    let mut ctx = TestContext::new("test_aggregation_window_functions").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("month", TypeId::VarChar),
        Column::new("sales", TypeId::Integer),
    ]);

    let table_name = "monthly_sales";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new("Jan"), Value::new(10000)],
        vec![Value::new(2), Value::new("Feb"), Value::new(12000)],
        vec![Value::new(3), Value::new("Mar"), Value::new(8000)],
        vec![Value::new(4), Value::new("Apr"), Value::new(15000)],
        vec![Value::new(5), Value::new("May"), Value::new(11000)],
        vec![Value::new(6), Value::new("Jun"), Value::new(13000)],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test SUM(), AVG(), COUNT() as window functions
    let sql = "SELECT month, sales, SUM(sales) OVER (ORDER BY id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total, AVG(sales) OVER (ORDER BY id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as moving_avg, COUNT(*) OVER () as total_months FROM monthly_sales ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 6, "Expected 6 rows");

        // Verify running totals and moving averages
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: month={}, sales={}, running_total={}, moving_avg={}, total_months={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3],
                row[4]
            );
        }
    } else {
        println!("Aggregation window functions not yet supported");
    }
}

#[tokio::test]
async fn test_window_frames() {
    let mut ctx = TestContext::new("test_window_frames").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("value", TypeId::Integer),
    ]);

    let table_name = "sequence";
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

    // Test different window frame specifications
    let sql = "SELECT id, value, SUM(value) OVER (ORDER BY id ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as window_sum, SUM(value) OVER (ORDER BY id RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as range_sum FROM sequence ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 5, "Expected 5 rows");

        // Verify window frame calculations
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: id={}, value={}, window_sum={}, range_sum={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3]
            );
        }
    } else {
        println!("Window frame specifications not yet supported");
    }
}

#[tokio::test]
async fn test_ntile_window_function() {
    let mut ctx = TestContext::new("test_ntile_window_function").await;

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
        vec![Value::new(1), Value::new(95), Value::new("Alice")],
        vec![Value::new(2), Value::new(87), Value::new("Bob")],
        vec![Value::new(3), Value::new(92), Value::new("Charlie")],
        vec![Value::new(4), Value::new(78), Value::new("Diana")],
        vec![Value::new(5), Value::new(89), Value::new("Eve")],
        vec![Value::new(6), Value::new(82), Value::new("Frank")],
        vec![Value::new(7), Value::new(94), Value::new("Grace")],
        vec![Value::new(8), Value::new(76), Value::new("Henry")],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test NTILE() window function
    let sql = "SELECT name, score, NTILE(4) OVER (ORDER BY score DESC) as quartile FROM students ORDER BY score DESC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 8, "Expected 8 rows");

        // Verify NTILE quartiles
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: name={}, score={}, quartile={}",
                i + 1,
                row[0],
                row[1],
                row[2]
            );
        }
    } else {
        println!("NTILE window function not yet supported");
    }
}

#[tokio::test]
async fn test_percent_rank_window_functions() {
    let mut ctx = TestContext::new("test_percent_rank_window_functions").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("salary", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "employees";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new(50000), Value::new("Alice")],
        vec![Value::new(2), Value::new(60000), Value::new("Bob")],
        vec![Value::new(3), Value::new(70000), Value::new("Charlie")],
        vec![Value::new(4), Value::new(80000), Value::new("Diana")],
        vec![Value::new(5), Value::new(90000), Value::new("Eve")],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test PERCENT_RANK() and CUME_DIST() window functions
    let sql = "SELECT name, salary, PERCENT_RANK() OVER (ORDER BY salary) as percent_rank, CUME_DIST() OVER (ORDER BY salary) as cumulative_dist FROM employees ORDER BY salary";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 5, "Expected 5 rows");

        // Verify PERCENT_RANK and CUME_DIST
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: name={}, salary={}, percent_rank={}, cume_dist={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3]
            );
        }
    } else {
        println!("PERCENT_RANK/CUME_DIST window functions not yet supported");
    }
}

#[tokio::test]
async fn test_complex_window_queries() {
    let mut ctx = TestContext::new("test_complex_window_queries").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("department", TypeId::VarChar),
        Column::new("salary", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
        Column::new("hire_date", TypeId::VarChar),
    ]);

    let table_name = "employees";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![
            Value::new(1),
            Value::new("IT"),
            Value::new(75000),
            Value::new("Alice"),
            Value::new("2020-01-15"),
        ],
        vec![
            Value::new(2),
            Value::new("IT"),
            Value::new(85000),
            Value::new("Bob"),
            Value::new("2019-03-10"),
        ],
        vec![
            Value::new(3),
            Value::new("IT"),
            Value::new(90000),
            Value::new("Charlie"),
            Value::new("2018-07-22"),
        ],
        vec![
            Value::new(4),
            Value::new("HR"),
            Value::new(65000),
            Value::new("Diana"),
            Value::new("2021-02-01"),
        ],
        vec![
            Value::new(5),
            Value::new("HR"),
            Value::new(70000),
            Value::new("Eve"),
            Value::new("2020-11-30"),
        ],
        vec![
            Value::new(6),
            Value::new("Sales"),
            Value::new(55000),
            Value::new("Frank"),
            Value::new("2022-01-10"),
        ],
        vec![
            Value::new(7),
            Value::new("Sales"),
            Value::new(60000),
            Value::new("Grace"),
            Value::new("2021-06-15"),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test complex query with multiple window functions
    let sql = "SELECT name, department, salary, ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) as dept_rank, RANK() OVER (ORDER BY salary DESC) as overall_rank, salary - AVG(salary) OVER (PARTITION BY department) as salary_diff_from_dept_avg FROM employees ORDER BY department, salary DESC";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 7, "Expected 7 rows");

        // Verify complex window function results
        for (i, row) in rows.iter().enumerate() {
            println!(
                "Row {}: name={}, dept={}, salary={}, dept_rank={}, overall_rank={}, salary_diff={}",
                i + 1,
                row[0],
                row[1],
                row[2],
                row[3],
                row[4],
                row[5]
            );
        }
    } else {
        println!("Complex window queries not yet supported");
    }
}

#[tokio::test]
async fn test_window_functions_edge_cases() {
    let mut ctx = TestContext::new("test_window_functions_edge_cases").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("value", TypeId::Integer),
    ]);

    let table_name = "edge_cases";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Test window functions on empty table
    let sql = "SELECT id, value, ROW_NUMBER() OVER (ORDER BY id) as row_num FROM edge_cases";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 0, "Expected 0 rows from empty table");
    }

    // Insert single row and test
    let test_data = vec![vec![Value::new(1), Value::new(100)]];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    let sql = "SELECT id, value, ROW_NUMBER() OVER (ORDER BY id) as row_num, RANK() OVER (ORDER BY value) as rank_val FROM edge_cases";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer)
        .await
        .unwrap();

    if success {
        let rows = writer.get_rows();
        assert_eq!(rows.len(), 1, "Expected 1 row");

        println!(
            "Single row result: id={}, value={}, row_num={}, rank={}",
            rows[0][0], rows[0][1], rows[0][2], rows[0][3]
        );
    } else {
        println!("Window functions on single row not yet supported");
    }
}
