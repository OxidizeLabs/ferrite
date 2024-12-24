use crate::catalogue::schema::Schema;
use crate::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::execution::plans::create_plan::CreateTablePlanNode;
use crate::execution::plans::delete_plan::DeleteNode;
use crate::execution::plans::filter_plan::FilterNode;
use crate::execution::plans::hash_join_plan::HashJoinNode;
use crate::execution::plans::index_scan_plan::IndexScanNode;
use crate::execution::plans::insert_plan::InsertNode;
use crate::execution::plans::limit_plan::LimitNode;
use crate::execution::plans::mock_scan_plan::MockScanNode;
use crate::execution::plans::nested_index_join_plan::NestedIndexJoinNode;
use crate::execution::plans::nested_loop_join_plan::NestedLoopJoinNode;
use crate::execution::plans::projection_plan::ProjectionNode;
use crate::execution::plans::seq_scan_plan::SeqScanPlanNode;
use crate::execution::plans::sort_plan::SortNode;
use crate::execution::plans::table_scan_plan::TableScanNode;
use crate::execution::plans::topn_per_group_plan::TopNPerGroupNode;
use crate::execution::plans::topn_plan::TopNNode;
use crate::execution::plans::update_plan::UpdateNode;
use crate::execution::plans::values_plan::ValuesNode;
use crate::execution::plans::window_plan::WindowNode;
use std::fmt;
use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub enum PlanType {
    SeqScan,
    IndexScan,
    Insert,
    Update,
    Delete,
    Aggregation,
    Limit,
    NestedLoopJoin,
    NestedIndexJoin,
    HashJoin,
    Filter,
    Values,
    Projection,
    Sort,
    TopN,
    TopNPerGroup,
    MockScan,
    // InitCheck,
    Window,
    CreateTable,
    TableScan,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    TableScan(TableScanNode),
    SeqScan(SeqScanPlanNode),
    IndexScan(IndexScanNode),
    Insert(InsertNode),
    Update(UpdateNode),
    Delete(DeleteNode),
    Aggregation(AggregationPlanNode),
    Limit(LimitNode),
    NestedLoopJoin(NestedLoopJoinNode),
    NestedIndexJoin(NestedIndexJoinNode),
    HashJoin(HashJoinNode),
    Filter(FilterNode),
    Values(ValuesNode),
    Projection(ProjectionNode),
    Sort(SortNode),
    TopN(TopNNode),
    TopNPerGroup(TopNPerGroupNode),
    MockScan(MockScanNode),
    Window(WindowNode),
    CreateTable(CreateTablePlanNode),
    Empty,
}

pub trait AbstractPlanNode {
    fn get_output_schema(&self) -> &Schema;
    fn get_children(&self) -> &Vec<PlanNode>;
    fn get_type(&self) -> PlanType;
    fn to_string(&self, with_schema: bool) -> String;
    fn plan_node_to_string(&self) -> String;
    fn children_to_string(&self, indent: usize) -> String;
}

impl AbstractPlanNode for PlanNode {
    fn get_output_schema(&self) -> &Schema {
        match self {
            _ => self.as_abstract_plan_node().get_output_schema(),
        }
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        match self {
            _ => self.as_abstract_plan_node().get_children(),
        }
    }

    fn get_type(&self) -> PlanType {
        match self {
            _ => self.as_abstract_plan_node().get_type(),
        }
    }

    fn to_string(&self, with_schema: bool) -> String {
        match self {
            _ => self.as_abstract_plan_node().to_string(with_schema),
        }
    }

    fn plan_node_to_string(&self) -> String {
        match self {
            _ => self.as_abstract_plan_node().plan_node_to_string(),
        }
    }

    fn children_to_string(&self, indent: usize) -> String {
        match self {
            _ => self.as_abstract_plan_node().children_to_string(indent),
        }
    }
}

impl PlanNode {
    pub fn explain(&self) -> String {
        self.explain_internal(0)
    }

    // Helper method to get a reference to the AbstractPlanNode
    fn as_abstract_plan_node(&self) -> &dyn AbstractPlanNode {
        match self {
            PlanNode::TableScan(node) => node,
            PlanNode::SeqScan(node) => node,
            PlanNode::IndexScan(node) => node,
            PlanNode::NestedLoopJoin(node) => node,
            PlanNode::Filter(node) => node,
            PlanNode::Aggregation(node) => node,
            PlanNode::Insert(node) => node,
            PlanNode::Update(node) => node,
            PlanNode::Delete(node) => node,
            PlanNode::Limit(node) => node,
            PlanNode::NestedIndexJoin(node) => node,
            PlanNode::HashJoin(node) => node,
            PlanNode::Values(node) => node,
            PlanNode::Projection(node) => node,
            PlanNode::Sort(node) => node,
            PlanNode::TopN(node) => node,
            PlanNode::TopNPerGroup(node) => node,
            PlanNode::MockScan(node) => node,
            PlanNode::Window(node) => node,
            PlanNode::CreateTable(node) => node,
            PlanNode::Empty => panic!("Empty plan node"),
        }
    }

    fn explain_internal(&self, depth: usize) -> String {
        let indent = "  ".repeat(depth);
        let mut result = String::new();

        match self {
            PlanNode::SeqScan(node) => {
                result.push_str(&format!("{}→ SeqScan\n", indent));
                result.push_str(&format!("{}   Table: {}\n", indent, node.get_table_name()));
                result.push_str(&format!("{}   Schema: {}\n", indent, node.get_output_schema()));
            }
            // PlanNode::IndexScan(node) => {
            //     result.push_str(&format!("{}→ IndexScan\n", indent));
            //     result.push_str(&format!("{}   Index: {}\n", indent, node.get_index_name()));
            //     result.push_str(&format!("{}   Schema: {}\n", indent, node.get_output_schema()));
            // }
            PlanNode::Filter(node) => {
                result.push_str(&format!("{}→ Filter\n", indent));
                result.push_str(&format!("{}   Predicate: {}\n", indent, node.get_filter_predicate()));
                result.push_str(&node.get_child_plan().explain_internal(depth + 1));
            }
            // PlanNode::HashJoin(node) => {
            //     result.push_str(&format!("{}→ HashJoin\n", indent));
            //     result.push_str(&format!("{}   Condition: {}\n", indent, node.get_join_predicate()));
            //     result.push_str(&format!("{}   Left Child:\n", indent));
            //     result.push_str(&node.get_left_child().explain_internal(depth + 1));
            //     result.push_str(&format!("{}   Right Child:\n", indent));
            //     result.push_str(&node.get_right_child().explain_internal(depth + 1));
            // }
            PlanNode::Aggregation(node) => {
                result.push_str(&format!("{}→ Aggregation\n", indent));
                result.push_str(&format!("{}   Group By: {:?}\n", indent, node.get_group_bys()));
                result.push_str(&format!("{}   Aggregates: {:?}\n", indent, node.get_aggregates()));
                for child in node.get_children() {
                    result.push_str(&child.explain_internal(depth + 1));
                }
            }
            PlanNode::Insert(node) => {
                result.push_str(&format!("{}→ Insert\n", indent));
                result.push_str(&format!("{}   Target Table: {}\n", indent, node.get_table_name()));
                result.push_str(&node.get_child().explain_internal(depth + 1));
            }
            PlanNode::Values(node) => {
                result.push_str(&format!("{}→ Values\n", indent));
                result.push_str(&format!("{}   Row Count: {}\n", indent, node.get_row_count()));
            }
            PlanNode::CreateTable(node) => {
                result.push_str(&format!("{}→ CreateTable\n", indent));
                result.push_str(&format!("{}   Table: {}\n", indent, node.get_table_name()));
                result.push_str(&format!("{}   Schema: {}\n", indent, node.get_output_schema()));
            }
            // PlanNode::NestedLoopJoin(node) => {
            //     result.push_str(&format!("{}→ NestedLoopJoin\n", indent));
            //     result.push_str(&format!("{}   Condition: {}\n", indent, node.get_join_predicate()));
            //     result.push_str(&format!("{}   Left Child:\n", indent));
            //     result.push_str(&node.get_left_child().explain_internal(depth + 1));
            //     result.push_str(&format!("{}   Right Child:\n", indent));
            //     result.push_str(&node.get_right_child().explain_internal(depth + 1));
            // }
            // PlanNode::Sort(node) => {
            //     result.push_str(&format!("{}→ Sort\n", indent));
            //     result.push_str(&format!("{}   Order By: {:?}\n", indent, node.get_sort_keys()));
            //     result.push_str(&node.get_child().explain_internal(depth + 1));
            // }
            // PlanNode::Limit(node) => {
            //     result.push_str(&format!("{}→ Limit\n", indent));
            //     result.push_str(&format!("{}   Limit: {}\n", indent, node.get_limit()));
            //     result.push_str(&node.get_child().explain_internal(depth + 1));
            // }
            PlanNode::Empty => {
                result.push_str(&format!("{}→ Empty\n", indent));
            }
            _ => result.push_str(&format!("{}→ Unknown Plan Node\n", indent)),
        }
        result
    }
}

impl Display for PlanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", AbstractPlanNode::to_string(self, true))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use std::collections::HashMap;
    use std::sync::Arc;
    use parking_lot::RwLock;
    use chrono::Utc;
    use tempfile::TempDir;
    use crate::catalogue::catalogue::Catalog;
    use crate::planner::planner::QueryPlanner;

    /// Test context that manages the lifetime of test resources
    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        planner: QueryPlanner,
        _temp_dir: TempDir,  // Holds directory reference to prevent deletion
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            // Create temporary directory for test files
            let temp_dir = TempDir::new().expect("Failed to create temp directory");

            // Create unique timestamp for test files
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();

            // Setup paths for database and log files
            let db_path = temp_dir.path().join(format!("{}_{}.db", test_name, timestamp));
            let log_path = temp_dir.path().join(format!("{}_{}.log", test_name, timestamp));

            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

            // Initialize disk manager with temp files
            let disk_manager = Arc::new(FileDiskManager::new(
                db_path.to_str().unwrap().to_string(),
                log_path.to_str().unwrap().to_string(),
                100
            ));

            // Setup buffer pool components
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let buffer_pool = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager,
                replacer,
            ));

            // Initialize catalog
            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool,
                0,              // next_index_oid
                0,              // next_table_oid
                HashMap::new(), // tables
                HashMap::new(), // indexes
                HashMap::new(), // table_names
                HashMap::new(), // index_names
            )));

            // Create query planner
            let planner = QueryPlanner::new(Arc::clone(&catalog));

            TestContext {
                catalog,
                planner,
                _temp_dir: temp_dir,
            }
        }

        fn setup_sample_tables(&mut self) -> Result<(), String> {
            // Create users table
            self.planner.create_plan(
                "CREATE TABLE users (
                    id INTEGER,
                    name VARCHAR(255),
                    age INTEGER,
                    email VARCHAR(255)
                )"
            )?;

            // Create orders table
            self.planner.create_plan(
                "CREATE TABLE orders (
                    id INTEGER,
                    user_id INTEGER,
                    amount INTEGER,
                    status VARCHAR(50)
                )"
            )?;

            // Create products table
            self.planner.create_plan(
                "CREATE TABLE products (
                    id INTEGER,
                    name VARCHAR(255),
                    price INTEGER,
                    category VARCHAR(100)
                )"
            )?;

            Ok(())
        }
    }

    #[test]
    fn test_basic_select_plan() {
        let mut ctx = TestContext::new("test_basic_select_plan");
        ctx.setup_sample_tables().unwrap();

        let test_cases = vec![
            (
                "SELECT id, name FROM users",
                vec!["SeqScan", "Table: users"]
            ),
            (
                "SELECT id FROM users WHERE age > 25",
                vec!["Filter", "SeqScan", "Table: users"]
            ),
        ];

        for (sql, expected_contents) in test_cases {
            let explanation = ctx.planner.explain(sql).unwrap();
            for expected in expected_contents {
                assert!(
                    explanation.contains(expected),
                    "Expected '{}' in plan for query '{}'\nActual plan:\n{}",
                    expected,
                    sql,
                    explanation
                );
            }
        }
    }

    #[test]
    fn test_simple_join_plan() {
        let mut ctx = TestContext::new("test_simple_join_plan");
        ctx.setup_sample_tables().unwrap();

        let test_cases = vec![
            (
                "SELECT users.id, orders.id
                 FROM users
                 INNER JOIN orders ON users.id = orders.user_id",
                vec!["HashJoin", "SeqScan", "Table: users", "Table: orders"]
            ),
        ];

        for (sql, expected_contents) in test_cases {
            let explanation = ctx.planner.explain(sql).unwrap();
            for expected in expected_contents {
                assert!(
                    explanation.contains(expected),
                    "Expected '{}' in plan for query '{}'\nActual plan:\n{}",
                    expected,
                    sql,
                    explanation
                );
            }
        }
    }

    #[test]
    fn test_simple_aggregation_plans() {
        let mut ctx = TestContext::new("test_simple_aggregation_plans");
        ctx.setup_sample_tables().unwrap();

        let test_cases = vec![
            (
                "SELECT COUNT(*) FROM users",
                vec!["Aggregation", "SeqScan", "Table: users"]
            ),
            (
                "SELECT age, COUNT(*) FROM users GROUP BY age",
                vec!["Aggregation", "SeqScan", "Table: users"]
            ),
        ];

        for (sql, expected_contents) in test_cases {
            let explanation = ctx.planner.explain(sql).unwrap();
            for expected in expected_contents {
                assert!(
                    explanation.contains(expected),
                    "Expected '{}' in plan for query '{}'\nActual plan:\n{}",
                    expected,
                    sql,
                    explanation
                );
            }
        }
    }

    #[test]
    fn test_filter_plan() {
        let mut ctx = TestContext::new("test_filter_plan");
        ctx.setup_sample_tables().unwrap();

        let test_cases = vec![
            (
                "SELECT id FROM users WHERE age > 25",
                vec!["Filter", "SeqScan", "Table: users"]
            ),
            (
                "SELECT id FROM users WHERE id < 10",
                vec!["Filter", "SeqScan", "Table: users"]
            ),
        ];

        for (sql, expected_contents) in test_cases {
            let explanation = ctx.planner.explain(sql).unwrap();
            for expected in expected_contents {
                assert!(
                    explanation.contains(expected),
                    "Expected '{}' in plan for query '{}'\nActual plan:\n{}",
                    expected,
                    sql,
                    explanation
                );
            }
        }
    }

    #[test]
    fn test_error_handling() {
        let mut ctx = TestContext::new("test_error_handling");
        ctx.setup_sample_tables().unwrap();

        // Test cases that should return errors
        let error_cases = vec![
            "SELECT * FROM nonexistent_table",
            "SELECT invalid_column FROM users",
            "SELECT * FROM users WHERE nonexistent_column = 1",
        ];

        for sql in error_cases {
            let result = ctx.planner.explain(sql);
            assert!(
                result.is_err(),
                "Expected error for query: {}",
                sql
            );
        }
    }

    #[test]
    fn test_plan_formatting() {
        let mut ctx = TestContext::new("test_plan_formatting");
        ctx.setup_sample_tables().unwrap();

        let sql = "SELECT id, age FROM users WHERE age > 25";

        let explanation = ctx.planner.explain(sql).unwrap();

        // Basic formatting checks
        assert!(explanation.contains("→"), "Plan should use arrow for hierarchy");
        assert!(explanation.contains("SeqScan"), "Plan should include SeqScan");
        assert!(explanation.contains("Filter"), "Plan should include Filter");

        // Check indentation (basic check)
        let lines: Vec<&str> = explanation.lines().collect();
        assert!(
            lines.iter().any(|line| line.contains("SeqScan") && line.starts_with("  ")),
            "Child nodes should be indented"
        );
    }

    #[test]
    fn test_table_schema_creation() {
        let mut ctx = TestContext::new("test_table_schema_creation");

        // Manually create a table and verify its schema
        let create_result = ctx.planner.create_plan(
            "CREATE TABLE test_table (
                id INTEGER,
                name VARCHAR(255),
                age INTEGER
            )"
        );

        assert!(create_result.is_ok(), "Table creation should succeed");

        // Try to use the newly created table in a query
        let explain_result = ctx.planner.explain("SELECT id, name FROM test_table");

        assert!(
            explain_result.is_ok(),
            "Query planning should succeed for newly created table"
        );
    }

    #[test]
    fn debug_column_resolution() {
        let mut ctx = TestContext::new("debug_column_resolution");
        ctx.setup_sample_tables().unwrap();

        // Detailed test to understand column resolution
        let test_cases = vec![
            "SELECT id FROM users",
            "SELECT name FROM users",
            "SELECT age FROM users",
            "SELECT id FROM users WHERE age > 25",
        ];

        for sql in test_cases {
            println!("Testing query: {}", sql);
            match ctx.planner.explain(sql) {
                Ok(explanation) => {
                    println!("Explanation:\n{}", explanation);
                },
                Err(e) => {
                    println!("Error in query '{}': {}", sql, e);
                    panic!("Query planning failed");
                }
            }
        }
    }

    #[test]
    fn test_basic_column_access() {
        let mut ctx = TestContext::new("test_basic_column_access");
        ctx.setup_sample_tables().unwrap();

        // Simplified test cases focusing on single column access
        let test_cases = vec![
            "SELECT id FROM users",
            "SELECT name FROM users",
        ];

        for sql in test_cases {
            let result = ctx.planner.explain(sql);
            assert!(
                result.is_ok(),
                "Query '{}' should succeed. Error: {:?}",
                sql,
                result.err()
            );
        }
    }

    #[test]
    fn comprehensive_table_schema_test() {
        let mut ctx = TestContext::new("comprehensive_table_schema_test");
        ctx.setup_sample_tables().unwrap();

        // Verify table schemas directly
        let catalog_guard = ctx.catalog.read();

        let tables = vec!["users", "orders", "products"];
        for table_name in tables {
            let table_info = catalog_guard.get_table(table_name)
                .expect(&format!("Table {} should exist", table_name));

            let schema = table_info.get_table_schema();

            println!("Schema for {}: {}", table_name, schema);
            assert!(
                schema.get_column_count() > 0,
                "Table {} should have columns",
                table_name
            );
        }
    }

    #[test]
    fn debug_schema_creation() {
        let mut ctx = TestContext::new("debug_schema_creation");

        // Manually create a table and print out details
        let create_result = ctx.planner.create_plan(
            "CREATE TABLE test_debug (
                id INTEGER,
                name VARCHAR(255),
                age INTEGER,
                email VARCHAR(255)
            )"
        );

        assert!(create_result.is_ok(), "Table creation should succeed");

        // Retrieve and print the table's schema
        let catalog_guard = ctx.catalog.read();
        let table_info = catalog_guard.get_table("test_debug")
            .expect("Table should exist in catalog");

        let schema = table_info.get_table_schema();

        println!("Schema details:");
        for i in 0..schema.get_column_count() {
            let column = schema.get_column(i.try_into().unwrap()).unwrap();
            println!("Column {}: Name = {}, Type = {:?}",
                     i,
                     column.get_name(),
                     column.get_type()
            );
        }

        // Verify schema has correct columns
        assert_eq!(schema.get_column_count(), 4, "Should have 4 columns");
        assert_eq!(
            schema.get_column_index("age").unwrap(),
            2,
            "Age column should be at index 2"
        );
    }

    #[test]
    fn verify_sample_tables_schema() {
        let mut ctx = TestContext::new("verify_sample_tables_schema");
        ctx.setup_sample_tables().unwrap();

        let catalog_guard = ctx.catalog.read();
        let tables = vec!["users", "orders", "products"];

        for table_name in tables {
            let table_info = catalog_guard.get_table(table_name)
                .expect(&format!("Table {} should exist", table_name));

            let schema = table_info.get_table_schema();

            println!("Schema for {}: ", table_name);
            for i in 0..schema.get_column_count() {
                let column = schema.get_column(i.try_into().unwrap()).unwrap();
                println!("Column {}: Name = {}, Type = {:?}",
                         i,
                         column.get_name(),
                         column.get_type()
                );
            }
        }
    }

    #[test]
    fn comprehensive_column_resolution_test() {
        let mut ctx = TestContext::new("comprehensive_column_resolution_test");
        ctx.setup_sample_tables().unwrap();

        // Test queries that should work
        let valid_queries = vec![
            "SELECT id FROM users",
            "SELECT name FROM users",
            "SELECT email FROM users",
        ];

        // Test queries that should fail
        let invalid_queries = vec![
            "SELECT age FROM products",  // column doesn't exist
            "SELECT nonexistent FROM users",  // column doesn't exist
        ];

        // Check valid queries
        for sql in valid_queries {
            let result = ctx.planner.explain(sql);
            assert!(
                result.is_ok(),
                "Query '{}' should succeed. Error: {:?}",
                sql,
                result.err()
            );
        }

        // Check invalid queries
        for sql in invalid_queries {
            let result = ctx.planner.explain(sql);
            assert!(
                result.is_err(),
                "Query '{}' should fail due to nonexistent column",
                sql
            );
        }
    }

    #[test]
    fn test_catalog_display() {
        let mut ctx = TestContext::new("test_catalog_display");
        ctx.setup_sample_tables().unwrap();

        // Print the entire catalog
        let catalog_guard = ctx.catalog.read();
        println!("{}", catalog_guard);

        // Verify that the display doesn't panic and contains expected information
        let catalog_string = format!("{}", catalog_guard);

        let tables = vec!["users", "orders", "products"];
        for table in tables {
            assert!(
                catalog_string.contains(table),
                "Catalog display should contain table: {}",
                table
            );
        }
    }
}