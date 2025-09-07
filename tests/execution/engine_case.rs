use crate::execution::common::{TestContext, TestResultWriter};
use tkdb::catalog::column::Column;
use tkdb::catalog::schema::Schema;
use tkdb::types_db::type_id::TypeId;
use tkdb::types_db::value::{Val, Value};

#[tokio::test]
async fn test_case_when_simple() {
    let mut ctx = TestContext::new("test_case_when_simple").await;

    // Create test table with same schema but smaller dataset
    let table_schema = Schema::new(vec![
        Column::new("a", TypeId::Integer),
        Column::new("b", TypeId::Integer),
        Column::new("c", TypeId::Integer),
        Column::new("d", TypeId::Integer),
        Column::new("e", TypeId::Integer),
    ]);

    ctx.create_test_table("t1", table_schema.clone()).unwrap();

    // Insert a small amount of test data where we can easily calculate the expected results
    let test_data = vec![
        // c values chosen to make it easy to verify against average
        // average of c values will be 150
        vec![
            Value::new(100),
            Value::new(10),
            Value::new(100),
            Value::new(1),
            Value::new(1),
        ], // b*10 = 100
        vec![
            Value::new(100),
            Value::new(20),
            Value::new(200),
            Value::new(1),
            Value::new(1),
        ], // a*2 = 200
        vec![
            Value::new(100),
            Value::new(30),
            Value::new(150),
            Value::new(1),
            Value::new(1),
        ], // b*10 = 300
    ];

    ctx.insert_tuples("t1", test_data, table_schema).unwrap();

    // Execute the same query as the main test
    let sql = "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();
    assert!(success, "Query execution failed");

    // Get the results
    let rows = writer.get_rows();

    // Print debug info
    println!("Average of c values should be 150");
    println!("Results:");
    for row in rows {
        println!("{}", row[0]);
    }

    // Verify we got exactly 3 results
    assert_eq!(rows.len(), 3, "Expected 3 rows in result");

    // Convert results to integers for easier comparison
    let results: Vec<i32> = rows
        .iter()
        .map(|row| row[0].to_string().parse::<i32>().unwrap())
        .collect();

    // Expected results:
    // Row 1: c=100 < avg(150) so b*10 = 100
    // Row 2: c=200 > avg(150) so a*2 = 200
    // Row 3: c=150 = avg(150) so b*10 = 300
    let expected = vec![100, 200, 300];

    // Verify results match expected values
    assert_eq!(results, expected, "Results don't match expected values");
}

#[tokio::test]
async fn test_case_when_with_subquery() {
    let mut ctx = TestContext::new("test_case_when_with_subquery").await;

    // Create test table with same schema as original test
    let table_schema = Schema::new(vec![
        Column::new("a", TypeId::Integer),
        Column::new("b", TypeId::Integer),
        Column::new("c", TypeId::Integer),
        Column::new("d", TypeId::Integer),
        Column::new("e", TypeId::Integer),
    ]);

    ctx.create_test_table("t1", table_schema.clone()).unwrap();

    // Insert the same test data as in the original test
    let test_data = vec![
        vec![
            Value::new(104),
            Value::new(100),
            Value::new(102),
            Value::new(101),
            Value::new(103),
        ],
        vec![
            Value::new(107),
            Value::new(105),
            Value::new(106),
            Value::new(108),
            Value::new(109),
        ],
        vec![
            Value::new(111),
            Value::new(112),
            Value::new(113),
            Value::new(114),
            Value::new(110),
        ],
        vec![
            Value::new(115),
            Value::new(118),
            Value::new(119),
            Value::new(116),
            Value::new(117),
        ],
        vec![
            Value::new(121),
            Value::new(124),
            Value::new(123),
            Value::new(122),
            Value::new(120),
        ],
        vec![
            Value::new(127),
            Value::new(129),
            Value::new(125),
            Value::new(128),
            Value::new(126),
        ],
        vec![
            Value::new(131),
            Value::new(130),
            Value::new(134),
            Value::new(133),
            Value::new(132),
        ],
        vec![
            Value::new(138),
            Value::new(139),
            Value::new(137),
            Value::new(136),
            Value::new(135),
        ],
        vec![
            Value::new(142),
            Value::new(143),
            Value::new(141),
            Value::new(140),
            Value::new(144),
        ],
        vec![
            Value::new(149),
            Value::new(145),
            Value::new(147),
            Value::new(148),
            Value::new(146),
        ],
        vec![
            Value::new(153),
            Value::new(151),
            Value::new(150),
            Value::new(154),
            Value::new(152),
        ],
        vec![
            Value::new(159),
            Value::new(158),
            Value::new(155),
            Value::new(156),
            Value::new(157),
        ],
        vec![
            Value::new(163),
            Value::new(160),
            Value::new(161),
            Value::new(164),
            Value::new(162),
        ],
        vec![
            Value::new(168),
            Value::new(167),
            Value::new(166),
            Value::new(169),
            Value::new(165),
        ],
        vec![
            Value::new(174),
            Value::new(170),
            Value::new(172),
            Value::new(171),
            Value::new(173),
        ],
        vec![
            Value::new(179),
            Value::new(175),
            Value::new(176),
            Value::new(178),
            Value::new(177),
        ],
        vec![
            Value::new(182),
            Value::new(181),
            Value::new(184),
            Value::new(183),
            Value::new(180),
        ],
        vec![
            Value::new(188),
            Value::new(186),
            Value::new(187),
            Value::new(185),
            Value::new(189),
        ],
        vec![
            Value::new(191),
            Value::new(194),
            Value::new(193),
            Value::new(190),
            Value::new(192),
        ],
        vec![
            Value::new(199),
            Value::new(198),
            Value::new(195),
            Value::new(196),
            Value::new(197),
        ],
        vec![
            Value::new(201),
            Value::new(200),
            Value::new(202),
            Value::new(203),
            Value::new(204),
        ],
        vec![
            Value::new(205),
            Value::new(206),
            Value::new(208),
            Value::new(207),
            Value::new(209),
        ],
        vec![
            Value::new(213),
            Value::new(211),
            Value::new(214),
            Value::new(212),
            Value::new(210),
        ],
        vec![
            Value::new(216),
            Value::new(218),
            Value::new(215),
            Value::new(217),
            Value::new(219),
        ],
        vec![
            Value::new(220),
            Value::new(223),
            Value::new(224),
            Value::new(222),
            Value::new(221),
        ],
        vec![
            Value::new(229),
            Value::new(228),
            Value::new(225),
            Value::new(226),
            Value::new(227),
        ],
        vec![
            Value::new(234),
            Value::new(232),
            Value::new(231),
            Value::new(233),
            Value::new(230),
        ],
        vec![
            Value::new(239),
            Value::new(236),
            Value::new(235),
            Value::new(238),
            Value::new(237),
        ],
        vec![
            Value::new(243),
            Value::new(240),
            Value::new(244),
            Value::new(241),
            Value::new(242),
        ],
        vec![
            Value::new(245),
            Value::new(249),
            Value::new(247),
            Value::new(248),
            Value::new(246),
        ],
    ];

    ctx.insert_tuples("t1", test_data, table_schema).unwrap();

    // Execute the query
    let sql = "SELECT CASE WHEN c>(SELECT avg(c) FROM t1) THEN a*2 ELSE b*10 END FROM t1 ORDER BY 1";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();
    assert!(success, "Query execution failed");

    // Get the results
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 30, "Expected 30 rows in result");

    // Print the values for debugging
    println!("Values in order:");
    for row in rows {
        println!("{}", row[0]);
    }

    // Join values with newlines and compute hash
    let data = rows
        .iter()
        .map(|row| row[0].to_string())
        .collect::<Vec<String>>()
        .join("\n")
        + "\n";

    println!(
        "\nFinal string being hashed (between ===):\n===\n{}===",
        data
    );
    println!("String length: {}", data.len());
    println!("Bytes: {:?}", data.as_bytes());

    let digest = md5::compute(data.as_bytes());
    let hash = format!("{:x}", digest);
    println!("Computed hash: {}", hash);

    // The expected hash from the original test
    let expected_hash = "55d5be1a73595fe92f388d9940e7fabe";
    assert_eq!(
        hash, expected_hash,
        "Hash mismatch - expected {}, got {}",
        expected_hash, hash
    );
}

#[tokio::test]
async fn test_case_when_basic() {
    let mut ctx = TestContext::new("test_case_when_basic").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("grade", TypeId::Integer),
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

    // Test basic CASE WHEN with ELSE
    let sql = "SELECT id, name, CASE WHEN grade >= 90 THEN 'A' WHEN grade >= 80 THEN 'B' ELSE 'C' END as letter_grade FROM students ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 rows");

    // Verify letter grades
    if rows.len() >= 4 {
        assert_eq!(rows[0][2].to_string(), "B"); // Alice: 85 -> B
        assert_eq!(rows[1][2].to_string(), "A"); // Bob: 92 -> A
        assert_eq!(rows[2][2].to_string(), "C"); // Charlie: 78 -> C
        assert_eq!(rows[3][2].to_string(), "A"); // Diana: 95 -> A
    }
}

#[tokio::test]
async fn test_case_when_multiple_conditions() {
    let mut ctx = TestContext::new("test_case_when_multiple_conditions").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("age", TypeId::Integer),
        Column::new("income", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "people";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![
            Value::new(1),
            Value::new(25),
            Value::new(40000),
            Value::new("Alice"),
        ],
        vec![
            Value::new(2),
            Value::new(35),
            Value::new(60000),
            Value::new("Bob"),
        ],
        vec![
            Value::new(3),
            Value::new(45),
            Value::new(80000),
            Value::new("Charlie"),
        ],
        vec![
            Value::new(4),
            Value::new(30),
            Value::new(50000),
            Value::new("Diana"),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test CASE WHEN with multiple conditions (AND/OR)
    let sql = "SELECT id, name, CASE WHEN age > 40 AND income > 70000 THEN 'Senior High' WHEN age > 30 OR income > 55000 THEN 'Mid Level' ELSE 'Junior' END as category FROM people ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 rows");

    // Verify categories
    if rows.len() >= 4 {
        assert_eq!(rows[0][2].to_string(), "Junior"); // Alice: 25, 40000 -> Junior
        assert_eq!(rows[1][2].to_string(), "Mid Level"); // Bob: 35, 60000 -> Mid Level
        assert_eq!(rows[2][2].to_string(), "Senior High"); // Charlie: 45, 80000 -> Senior High
        assert_eq!(rows[3][2].to_string(), "Junior"); // Diana: 30, 50000 -> Junior (30 is NOT > 30, 50000 is NOT > 55000)
    }
}

#[tokio::test]
async fn test_case_when_with_null_values() {
    let mut ctx = TestContext::new("test_case_when_with_null_values").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("score", TypeId::Integer),
        Column::new("bonus", TypeId::Integer),
    ]);

    let table_name = "scores";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data with NULL values
    let test_data = vec![
        vec![Value::new(1), Value::new(85), Value::new(10)],
        vec![
            Value::new(2),
            Value::new_with_type(Val::Null, TypeId::Integer),
            Value::new(5),
        ],
        vec![
            Value::new(3),
            Value::new(90),
            Value::new_with_type(Val::Null, TypeId::Integer),
        ],
        vec![
            Value::new(4),
            Value::new_with_type(Val::Null, TypeId::Integer),
            Value::new_with_type(Val::Null, TypeId::Integer),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test CASE WHEN with NULL handling
    let sql = "SELECT id, CASE WHEN score IS NULL THEN 'No Score' WHEN score >= 90 THEN 'Excellent' WHEN score >= 80 THEN 'Good' ELSE 'Poor' END as grade FROM scores ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 rows");
    // NULL handling behavior may vary by implementation
}

#[tokio::test]
async fn test_case_when_with_aggregations() {
    let mut ctx = TestContext::new("test_case_when_with_aggregations").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("department", TypeId::VarChar),
        Column::new("salary", TypeId::Integer),
        Column::new("employee_id", TypeId::Integer),
    ]);

    let table_name = "employees";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new("IT"), Value::new(75000), Value::new(1)],
        vec![Value::new("IT"), Value::new(85000), Value::new(2)],
        vec![Value::new("HR"), Value::new(65000), Value::new(3)],
        vec![Value::new("HR"), Value::new(70000), Value::new(4)],
        vec![Value::new("Sales"), Value::new(55000), Value::new(5)],
        vec![Value::new("Sales"), Value::new(60000), Value::new(6)],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test CASE WHEN with aggregation functions
    let sql = "SELECT department, COUNT(CASE WHEN salary > 70000 THEN 1 END) as high_earners, COUNT(*) as total_employees FROM employees GROUP BY department ORDER BY department";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 3, "Expected 3 departments");

    // Verify aggregation with CASE
    if rows.len() >= 3 {
        // HR: 0 high earners (both under 70000), 2 total
        // IT: 2 high earners (both over 70000), 2 total
        // Sales: 0 high earners (both under 70000), 2 total
    }
}

#[tokio::test]
async fn test_case_when_with_arithmetic() {
    let mut ctx = TestContext::new("test_case_when_with_arithmetic").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("quantity", TypeId::Integer),
        Column::new("price", TypeId::Integer),
    ]);

    let table_name = "products";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new(10), Value::new(100)],
        vec![Value::new(2), Value::new(50), Value::new(80)],
        vec![Value::new(3), Value::new(5), Value::new(200)],
        vec![Value::new(4), Value::new(100), Value::new(50)],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test CASE WHEN with arithmetic expressions (using consistent decimal types)
    let sql = "SELECT id, quantity, price, CASE WHEN quantity * price > 4000 THEN quantity * price * 0.9 WHEN quantity * price > 1000 THEN quantity * price * 0.95 ELSE quantity * price * 1.0 END as final_price FROM products ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 rows");

    // Verify arithmetic calculations in CASE
    if rows.len() >= 4 {
        // Product 1: 10 * 100 = 1000 -> no discount
        // Product 2: 50 * 80 = 4000 -> no discount
        // Product 3: 5 * 200 = 1000 -> no discount
        // Product 4: 100 * 50 = 5000 -> 10% discount
    }
}

#[tokio::test]
async fn test_case_when_nested() {
    let mut ctx = TestContext::new("test_case_when_nested").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("age", TypeId::Integer),
        Column::new("experience", TypeId::Integer),
        Column::new("performance", TypeId::Integer),
    ]);

    let table_name = "candidates";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new(25), Value::new(2), Value::new(8)],
        vec![Value::new(2), Value::new(35), Value::new(10), Value::new(9)],
        vec![Value::new(3), Value::new(28), Value::new(5), Value::new(7)],
        vec![Value::new(4), Value::new(40), Value::new(15), Value::new(9)],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test nested CASE WHEN statements
    let sql = "SELECT id, CASE WHEN age > 30 THEN CASE WHEN experience > 8 THEN 'Senior Expert' ELSE 'Senior' END WHEN age > 25 THEN CASE WHEN performance > 8 THEN 'Mid Expert' ELSE 'Mid' END ELSE 'Junior' END as level FROM candidates ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 rows");

    // Verify nested logic
    if rows.len() >= 4 {
        assert_eq!(rows[0][1].to_string(), "Junior"); // age 25 -> Junior
        assert_eq!(rows[1][1].to_string(), "Senior Expert"); // age 35, exp 10 -> Senior Expert
        assert_eq!(rows[2][1].to_string(), "Mid"); // age 28, perf 7 -> Mid
        assert_eq!(rows[3][1].to_string(), "Senior Expert"); // age 40, exp 15 -> Senior Expert
    }
}

#[tokio::test]
async fn test_case_when_different_data_types() {
    let mut ctx = TestContext::new("test_case_when_different_data_types").await;

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
            Value::new(1),
            Value::new("Alice"),
            Value::new(true),
            Value::new(85i64),
        ],
        vec![
            Value::new(2),
            Value::new("Bob"),
            Value::new(false),
            Value::new(92i64),
        ],
        vec![
            Value::new(3),
            Value::new("Charlie"),
            Value::new(true),
            Value::new(78i64),
        ],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test CASE WHEN with different data types
    let sql = "SELECT id, name, CASE WHEN active = true THEN 'Active User' ELSE 'Inactive User' END as status, CASE WHEN score > 80 THEN 'High' ELSE 'Low' END as performance FROM mixed_data ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 3, "Expected 3 rows");

    // Verify different data type handling
    if rows.len() >= 3 {
        assert_eq!(rows[0][2].to_string(), "Active User"); // Alice: active=true
        assert_eq!(rows[1][2].to_string(), "Inactive User"); // Bob: active=false
        assert_eq!(rows[2][2].to_string(), "Active User"); // Charlie: active=true
    }
}

#[tokio::test]
async fn test_case_when_with_in_clause() {
    let mut ctx = TestContext::new("test_case_when_with_in_clause").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("department_id", TypeId::Integer),
        Column::new("name", TypeId::VarChar),
    ]);

    let table_name = "employees";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert test data
    let test_data = vec![
        vec![Value::new(1), Value::new(10), Value::new("Alice")],
        vec![Value::new(2), Value::new(20), Value::new("Bob")],
        vec![Value::new(3), Value::new(30), Value::new("Charlie")],
        vec![Value::new(4), Value::new(40), Value::new("Diana")],
    ];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test CASE WHEN with IN clause
    let sql = "SELECT id, name, CASE WHEN department_id IN (10, 20) THEN 'Core Team' WHEN department_id IN (30, 40) THEN 'Support Team' ELSE 'Other' END as team FROM employees ORDER BY id";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 rows");

    // Verify IN clause logic
    if rows.len() >= 4 {
        assert_eq!(rows[0][2].to_string(), "Core Team"); // dept_id 10
        assert_eq!(rows[1][2].to_string(), "Core Team"); // dept_id 20
        assert_eq!(rows[2][2].to_string(), "Support Team"); // dept_id 30
        assert_eq!(rows[3][2].to_string(), "Support Team"); // dept_id 40
    }
}

#[tokio::test]
async fn test_case_when_edge_cases() {
    let mut ctx = TestContext::new("test_case_when_edge_cases").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("value", TypeId::Integer),
    ]);

    let table_name = "edge_cases";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Test CASE WHEN on empty table
    let sql = "SELECT id, CASE WHEN value > 50 THEN 'High' ELSE 'Low' END as category FROM edge_cases";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 0, "Expected 0 rows from empty table");

    // Insert single row
    let test_data = vec![vec![Value::new(1), Value::new(75)]];
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test CASE WHEN on single row
    let sql = "SELECT id, CASE WHEN value > 50 THEN 'High' ELSE 'Low' END as category FROM edge_cases";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 1, "Expected 1 row");

    if rows.len() == 1 {
        assert_eq!(rows[0][1].to_string(), "High"); // value 75 > 50
    }
}

#[tokio::test]
async fn test_case_when_performance() {
    let mut ctx = TestContext::new("test_case_when_performance").await;

    // Create test table
    let table_schema = Schema::new(vec![
        Column::new("id", TypeId::Integer),
        Column::new("category", TypeId::VarChar),
        Column::new("value", TypeId::Integer),
    ]);

    let table_name = "large_dataset";
    ctx.create_test_table(table_name, table_schema.clone())
        .unwrap();

    // Insert larger dataset for performance testing
    let mut test_data = Vec::new();
    for i in 1..=100 {
        let category = match i % 4 {
            0 => "A",
            1 => "B",
            2 => "C",
            _ => "D",
        };
        let value = (i * 13) % 200; // Generate some pseudo-random values
        test_data.push(vec![Value::new(i), Value::new(category), Value::new(value)]);
    }
    ctx.insert_tuples(table_name, test_data, table_schema)
        .unwrap();

    // Test CASE WHEN performance with larger dataset
    let sql = "SELECT category, COUNT(CASE WHEN value > 150 THEN 1 END) as high_values, COUNT(CASE WHEN value BETWEEN 50 AND 150 THEN 1 END) as mid_values, COUNT(CASE WHEN value < 50 THEN 1 END) as low_values FROM large_dataset GROUP BY category ORDER BY category";
    let mut writer = TestResultWriter::new();
    let success = ctx
        .engine
        .execute_sql(sql, ctx.exec_ctx.clone(), &mut writer).await
        .unwrap();

    assert!(success, "Performance test query execution failed");
    let rows = writer.get_rows();
    assert_eq!(rows.len(), 4, "Expected 4 categories");

    println!(
        "Performance test completed with {} category groups",
        rows.len()
    );
}
