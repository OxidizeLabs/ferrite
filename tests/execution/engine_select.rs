use crate::common::logger::init_test_logger;
use crate::common::tempdb::new_temp_db;
use super::common::TestResultWriter;
use ferrite::concurrency::transaction::IsolationLevel;

#[tokio::test]
async fn select_basic_projection() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = TestResultWriter::new();

    db.execute_sql(
        "CREATE TABLE widgets (id INTEGER, label VARCHAR(32));",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO widgets VALUES (2, 'two'), (1, 'one'), (3, 'three');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    let mut result_writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT id, label FROM widgets ORDER BY id;",
        IsolationLevel::ReadCommitted,
        &mut result_writer,
    )
    .await
    .unwrap();

    let rows = result_writer.get_rows();
    let actual: Vec<(String, String)> = rows
        .iter()
        .map(|row| (row[0].to_string(), row[1].to_string()))
        .collect();

    assert_eq!(
        actual,
        vec![
            ("1".to_string(), "one".to_string()),
            ("2".to_string(), "two".to_string()),
            ("3".to_string(), "three".to_string())
        ],
        "Basic SELECT should return all rows with projected columns in order"
    );
}

#[tokio::test]
async fn select_where_order_by() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = TestResultWriter::new();

    db.execute_sql(
        "CREATE TABLE t (id INTEGER, name VARCHAR(32), v INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO t VALUES (3, 'c', 30), (1, 'a', 10), (2, 'b', 20);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    let mut result_writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT id, name FROM t WHERE v >= 20 ORDER BY id;",
        IsolationLevel::ReadCommitted,
        &mut result_writer,
    )
    .await
    .unwrap();

    let rows = result_writer.get_rows();
    let actual: Vec<(String, String)> = rows
        .iter()
        .map(|row| (row[0].to_string(), row[1].to_string()))
        .collect();
    assert_eq!(
        actual,
        vec![("2".to_string(), "b".to_string()), ("3".to_string(), "c".to_string())],
        "SELECT with WHERE and ORDER BY should project filtered rows in order"
    );
}

#[tokio::test]
async fn select_projection_and_expressions() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = TestResultWriter::new();

    db.execute_sql(
        "CREATE TABLE metrics (id INTEGER, name VARCHAR(32), score INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO metrics VALUES (1, 'alpha', 10), (2, 'beta', 20), (3, 'gamma', 15);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    let mut result_writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT name AS label, score, score * 2 AS doubled, score + id AS score_plus_id FROM metrics ORDER BY score DESC;",
        IsolationLevel::ReadCommitted,
        &mut result_writer,
    )
    .await
    .unwrap();

    let rows = result_writer.get_rows();
    let actual: Vec<Vec<String>> = rows
        .iter()
        .map(|row| row.iter().map(|value| value.to_string()).collect())
        .collect();

    let expected = vec![
        vec![
            "beta".to_string(),
            "20".to_string(),
            "40".to_string(),
            "22".to_string(),
        ],
        vec![
            "gamma".to_string(),
            "15".to_string(),
            "30".to_string(),
            "18".to_string(),
        ],
        vec![
            "alpha".to_string(),
            "10".to_string(),
            "20".to_string(),
            "11".to_string(),
        ],
    ];

    assert_eq!(
        actual, expected,
        "SELECT should project columns, aliases, and computed expressions in order"
    );
}

#[tokio::test]
async fn select_distinct_rows() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = TestResultWriter::new();

    db.execute_sql(
        "CREATE TABLE events (category VARCHAR(16), severity INTEGER);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO events VALUES
            ('auth', 1),
            ('auth', 1),
            ('auth', 2),
            ('billing', 1),
            ('billing', 1),
            ('ui', 3);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    let mut categories_writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT DISTINCT category FROM events ORDER BY category;",
        IsolationLevel::ReadCommitted,
        &mut categories_writer,
    )
    .await
    .unwrap();

    let categories: Vec<String> = categories_writer
        .get_rows()
        .iter()
        .map(|row| row[0].to_string())
        .collect();
    assert_eq!(
        categories,
        vec!["auth".to_string(), "billing".to_string(), "ui".to_string()],
        "DISTINCT should remove duplicate category values"
    );

    let mut pairs_writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT DISTINCT category, severity FROM events ORDER BY category, severity;",
        IsolationLevel::ReadCommitted,
        &mut pairs_writer,
    )
    .await
    .unwrap();

    let distinct_pairs: Vec<(String, String)> = pairs_writer
        .get_rows()
        .iter()
        .map(|row| (row[0].to_string(), row[1].to_string()))
        .collect();
    let expected_pairs = vec![
        ("auth".to_string(), "1".to_string()),
        ("auth".to_string(), "2".to_string()),
        ("billing".to_string(), "1".to_string()),
        ("ui".to_string(), "3".to_string()),
    ];

    assert_eq!(
        distinct_pairs, expected_pairs,
        "DISTINCT across multiple columns should preserve unique combinations"
    );
}

#[tokio::test]
async fn select_order_by_full_dataset() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = TestResultWriter::new();

    db.execute_sql(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, city TEXT);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO users (id, name, age, city) VALUES
            (1, 'Alice', 30, 'Seattle'),
            (2, 'Bob', 25, 'Portland'),
            (3, 'Carol', 35, 'Seattle'),
            (4, 'Dave', 40, 'San Francisco'),
            (5, 'Eve', 28, 'Portland');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    let mut result_writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT name FROM users ORDER BY name;",
        IsolationLevel::ReadCommitted,
        &mut result_writer,
    )
    .await
    .unwrap();

    let names: Vec<String> = result_writer
        .get_rows()
        .iter()
        .map(|row| row[0].to_string())
        .collect();

    assert_eq!(
        names,
        vec![
            "Alice".to_string(),
            "Bob".to_string(),
            "Carol".to_string(),
            "Dave".to_string(),
            "Eve".to_string(),
        ],
        "SELECT with ORDER BY should return all inserted rows in alphabetical order"
    );
}

#[tokio::test]
async fn select_order_by_age_desc_limit() {
    init_test_logger();
    let db = new_temp_db().await.unwrap();
    let mut writer = TestResultWriter::new();

    db.execute_sql(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, age INTEGER, city TEXT);",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    db.execute_sql(
        "INSERT INTO users (id, name, age, city) VALUES
            (1, 'Alice', 30, 'Seattle'),
            (2, 'Bob', 25, 'Portland'),
            (3, 'Carol', 35, 'Seattle'),
            (4, 'Dave', 40, 'San Francisco'),
            (5, 'Eve', 28, 'Portland');",
        IsolationLevel::ReadCommitted,
        &mut writer,
    )
    .await
    .unwrap();

    let mut result_writer = TestResultWriter::new();
    db.execute_sql(
        "SELECT name FROM users ORDER BY age DESC, name LIMIT 2;",
        IsolationLevel::ReadCommitted,
        &mut result_writer,
    )
    .await
    .unwrap();

    let names: Vec<String> = result_writer
        .get_rows()
        .iter()
        .map(|row| row[0].to_string())
        .collect();

    assert_eq!(
        names,
        vec!["Dave".to_string(), "Carol".to_string()],
        "ORDER BY with LIMIT should respect ordering on multiple columns"
    );
}
