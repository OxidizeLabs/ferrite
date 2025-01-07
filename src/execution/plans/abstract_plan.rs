use crate::catalogue::schema::Schema;
use crate::execution::plans::aggregation_plan::AggregationPlanNode;
use crate::execution::plans::create_index_plan::CreateIndexPlanNode;
use crate::execution::plans::create_table_plan::CreateTablePlanNode;
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
    CreateIndex,
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
    CreateIndex(CreateIndexPlanNode),
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
            PlanNode::CreateIndex(node) => node,
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
            PlanNode::IndexScan(node) => {
                result.push_str(&format!("{}→ IndexScan\n", indent));
                result.push_str(&format!("{}   Index: {}\n", indent, node.get_index_name()));
                result.push_str(&format!("{}   Schema: {}\n", indent, node.get_output_schema()));
            }
            PlanNode::Filter(node) => {
                result.push_str(&format!("{}→ Filter\n", indent));
                result.push_str(&format!("{}   Predicate: {}\n", indent, node.get_filter_predicate()));
                result.push_str(&node.get_child_plan().explain_internal(depth + 1));
            }
            PlanNode::HashJoin(node) => {
                result.push_str(&format!("{}→ HashJoin\n", indent));
                result.push_str(&format!("{}   Condition: {}\n", indent, node.get_join()));
                result.push_str(&format!("{}   Left Child:\n", indent));
                result.push_str(&node.get_left_child().explain_internal(depth + 1));
                result.push_str(&format!("{}   Right Child:\n", indent));
                result.push_str(&node.get_right_child().explain_internal(depth + 1));
            }
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
            PlanNode::NestedLoopJoin(node) => {
                result.push_str(&format!("{}→ NestedLoopJoin\n", indent));
                result.push_str(&format!("{}   Condition: {}\n", indent, node.get_join()));
                result.push_str(&format!("{}   Left Child:\n", indent));
                result.push_str(&node.get_left_child().explain_internal(depth + 1));
                result.push_str(&format!("{}   Right Child:\n", indent));
                result.push_str(&node.get_right_child().explain_internal(depth + 1));
            }
            PlanNode::Sort(node) => {
                result.push_str(&format!("{}→ Sort\n", indent));
                result.push_str(&format!("{}   Order By: {:?}\n", indent, node.get_order_bys()));
                result.push_str(&node.get_children().iter().map(|_| self.explain_internal(depth + 1)).collect::<Vec<String>>().join(" "));
            }
            PlanNode::Limit(node) => {
                result.push_str(&format!("{}→ Limit\n", indent));
                result.push_str(&format!("{}   Limit: {}\n", indent, node.get_limit()));
                result.push_str(&node.get_children().iter().map(|_| self.explain_internal(depth + 1)).collect::<Vec<String>>().join(" "));
            }
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
mod basic_behaviour {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::catalogue::Catalog;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::planner::planner::QueryPlanner;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use log::{info, warn};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        planner: QueryPlanner,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_path = temp_dir.path().join(format!("{}_{}.db", test_name, timestamp));
            let log_path = temp_dir.path().join(format!("{}_{}.log", test_name, timestamp));

            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

            let disk_manager = Arc::new(FileDiskManager::new(
                db_path.to_str().unwrap().to_string(),
                log_path.to_str().unwrap().to_string(),
                100,
            ));

            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let buffer_pool = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager,
                replacer,
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool,
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )));

            let planner = QueryPlanner::new(Arc::clone(&catalog));

            TestContext {
                catalog,
                planner,
                _temp_dir: temp_dir,
            }
        }

        fn setup_tables(&mut self) -> Result<(), Box<dyn Error>> {
            let mut catalog = self.catalog.write();

            // Create users table schema
            let users_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
                Column::new("email", TypeId::VarChar),
            ]);

            // Create orders table schema
            let orders_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("user_id", TypeId::Integer),
                Column::new("amount", TypeId::Integer),
                Column::new("status", TypeId::VarChar),
            ]);

            // Create products table schema
            let products_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("price", TypeId::Integer),
                Column::new("category", TypeId::VarChar),
            ]);

            info!("Creating tables...");

            // Create tables in catalog
            let users_info = catalog.create_table("users", users_schema.clone())
                .ok_or("Failed to create users table")?;
            info!("Created users table: {}", users_info.get_table_name());
            info!("Users schema: {:?}", users_schema);

            let orders_info = catalog.create_table("orders", orders_schema.clone())
                .ok_or("Failed to create orders table")?;
            info!("Created orders table: {}", orders_info.get_table_name());

            let products_info = catalog.create_table("products", products_schema.clone())
                .ok_or("Failed to create products table")?;
            info!("Created products table: {}", products_info.get_table_name());

            Ok(())
        }

        fn debug_plan(&mut self, sql: &str) -> Result<(), Box<dyn Error>> {
            info!("Generating plan for SQL: {}", sql);
            match self.planner.explain(sql) {
                Ok(plan) => {
                    info!("Generated plan:\n{}", plan);
                    Ok(())
                }
                Err(e) => {
                    warn!("Failed to generate plan: {}", e);
                    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))
                }
            }
        }
    }

    #[test]
    fn test_simple_selects() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_simple_selects");
        ctx.setup_tables()?;

        let test_cases = vec![
            "SELECT id FROM users",
            "SELECT id, name FROM users",
            "SELECT * FROM users",
            "SELECT id as user_id FROM users",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
            assert!(plan.contains("users"), "Plan should reference users table");
        }

        Ok(())
    }

    #[test]
    fn test_basic_filters() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_basic_filters");
        ctx.setup_tables()?;

        let test_cases = vec![
            "SELECT id FROM users WHERE age > 25",
            "SELECT id FROM users WHERE age >= 20 AND age <= 30",
            "SELECT id FROM users WHERE age > 25 AND name = 'John'",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
            assert!(plan.contains("users"), "Plan should reference users table");
        }

        Ok(())
    }

    #[test]
    fn test_basic_aggregations() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_basic_aggregations");
        ctx.setup_tables()?;

        let test_cases = vec![
            "SELECT COUNT(*) FROM users",
            "SELECT age, COUNT(*) FROM users GROUP BY age",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
            assert!(plan.contains("users"), "Plan should reference users table");
        }

        Ok(())
    }

    #[test]
    fn test_error_handling() {
        let mut ctx = TestContext::new("test_error_handling");
        ctx.setup_tables().unwrap();

        let error_cases = vec![
            // Invalid table
            ("SELECT * FROM nonexistent_table", "not found in catalog"),

            // Invalid column
            ("SELECT nonexistent FROM users", "not found in schema"),

            // Invalid where clause column
            ("SELECT * FROM users WHERE nonexistent > 0", "not found in schema"),
        ];

        for (sql, expected_error) in error_cases {
            info!("Testing error case: {}", sql);
            let result = ctx.planner.explain(sql);
            assert!(result.is_err(), "Query should fail: {}", sql);
            let error = result.unwrap_err();
            assert!(
                error.to_string().contains(expected_error),
                "Error '{}' should contain '{}' for query: {}",
                error,
                expected_error,
                sql
            );
        }
    }

    #[test]
    fn test_schema_validation() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_schema_validation");
        ctx.setup_tables()?;

        let catalog = ctx.catalog.read();

        // Verify users table exists and has correct schema
        let users_table = catalog.get_table("users")
            .ok_or("Users table not found")?;
        let users_schema = users_table.get_table_schema();

        info!("Users table schema: {:?}", users_schema);

        assert_eq!(users_schema.get_column_count(), 4);
        assert!(users_schema.get_column_index("id").is_some());
        assert!(users_schema.get_column_index("name").is_some());
        assert!(users_schema.get_column_index("age").is_some());
        assert!(users_schema.get_column_index("email").is_some());

        Ok(())
    }
}

#[cfg(test)]
mod complex_behaviour {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::catalogue::Catalog;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::planner::planner::QueryPlanner;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use log::{info, warn};
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::error::Error;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct TestContext {
        catalog: Arc<RwLock<Catalog>>,
        planner: QueryPlanner,
        _temp_dir: TempDir,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            let temp_dir = TempDir::new().expect("Failed to create temp directory");
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_path = temp_dir.path().join(format!("{}_{}.db", test_name, timestamp));
            let log_path = temp_dir.path().join(format!("{}_{}.log", test_name, timestamp));

            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

            let disk_manager = Arc::new(FileDiskManager::new(
                db_path.to_str().unwrap().to_string(),
                log_path.to_str().unwrap().to_string(),
                100,
            ));

            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let buffer_pool = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                disk_scheduler,
                disk_manager,
                replacer,
            ));

            let catalog = Arc::new(RwLock::new(Catalog::new(
                buffer_pool,
                0,
                0,
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
            )));

            let planner = QueryPlanner::new(Arc::clone(&catalog));

            TestContext {
                catalog,
                planner,
                _temp_dir: temp_dir,
            }
        }

        fn setup_tables(&mut self) -> Result<(), Box<dyn Error>> {
            let mut catalog = self.catalog.write();

            // Create users table schema
            let users_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("age", TypeId::Integer),
                Column::new("email", TypeId::VarChar),
            ]);

            // Create orders table schema
            let orders_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("user_id", TypeId::Integer),
                Column::new("amount", TypeId::Integer),
                Column::new("status", TypeId::VarChar),
                Column::new("created_at", TypeId::Integer),
            ]);

            // Create products table schema
            let products_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("price", TypeId::Integer),
                Column::new("category", TypeId::VarChar),
                Column::new("stock", TypeId::Integer),
            ]);

            // Create order_items table schema
            let order_items_schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("order_id", TypeId::Integer),
                Column::new("product_id", TypeId::Integer),
                Column::new("quantity", TypeId::Integer),
                Column::new("price", TypeId::Integer),
            ]);

            info!("Creating tables...");

            // Create tables in catalog
            catalog.create_table("users", users_schema.clone())
                .ok_or("Failed to create users table")?;

            catalog.create_table("orders", orders_schema.clone())
                .ok_or("Failed to create orders table")?;

            catalog.create_table("products", products_schema.clone())
                .ok_or("Failed to create products table")?;

            catalog.create_table("order_items", order_items_schema.clone())
                .ok_or("Failed to create order_items table")?;

            Ok(())
        }

        fn debug_plan(&mut self, sql: &str) -> Result<(), Box<dyn Error>> {
            info!("Generating plan for SQL: {}", sql);
            match self.planner.explain(sql) {
                Ok(plan) => {
                    info!("Generated plan:\n{}", plan);
                    Ok(())
                }
                Err(e) => {
                    warn!("Failed to generate plan: {}", e);
                    Err(Box::new(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())))
                }
            }
        }
    }

    #[test]
    fn test_simple_selects() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_simple_selects");
        ctx.setup_tables()?;

        let test_cases = vec![
            "SELECT id FROM users",
            "SELECT id, name FROM users",
            "SELECT * FROM users",
            "SELECT DISTINCT name FROM users",
            "SELECT id as user_id FROM users",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
            assert!(plan.contains("users"), "Plan should reference users table");
        }

        Ok(())
    }

    #[test]
    fn test_filters() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_filters");
        ctx.setup_tables()?;

        let test_cases = vec![
            "SELECT id FROM users WHERE age > 25",
            "SELECT id FROM users WHERE age BETWEEN 20 AND 30",
            "SELECT id FROM users WHERE name LIKE 'John%'",
            "SELECT id FROM users WHERE age IN (25, 30, 35)",
            "SELECT id FROM users WHERE age > 25 AND name LIKE 'John%'",
            "SELECT id FROM users WHERE age > 25 OR name LIKE 'John%'",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
            assert!(plan.contains("users"), "Plan should reference users table");
        }

        Ok(())
    }

    #[test]
    fn test_joins() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_joins");
        ctx.setup_tables()?;

        let test_cases = vec![
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id",
            "SELECT u.name, o.amount FROM users u LEFT JOIN orders o ON u.id = o.user_id",
            "SELECT u.name, p.name FROM users u JOIN orders o ON u.id = o.user_id JOIN products p ON p.id = o.product_id",
            "SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id WHERE o.amount > 1000",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
            assert!(plan.contains("JOIN"), "Plan should contain JOIN operation");
        }

        Ok(())
    }

    #[test]
    fn test_aggregations() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_aggregations");
        ctx.setup_tables()?;

        let test_cases = vec![
            "SELECT COUNT(*) FROM users",
            "SELECT age, COUNT(*) FROM users GROUP BY age",
            "SELECT age, COUNT(*) FROM users GROUP BY age HAVING COUNT(*) > 5",
            "SELECT category, AVG(price) FROM products GROUP BY category",
            "SELECT user_id, SUM(amount) FROM orders GROUP BY user_id",
            "SELECT category, COUNT(*), AVG(price) FROM products GROUP BY category HAVING COUNT(*) > 2",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
        }

        Ok(())
    }

    #[test]
    fn test_sorting_and_limits() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_sorting_and_limits");
        ctx.setup_tables()?;

        let test_cases = vec![
            "SELECT * FROM users ORDER BY age DESC",
            "SELECT * FROM users ORDER BY age ASC, name DESC",
            "SELECT * FROM users LIMIT 10",
            "SELECT * FROM users ORDER BY age DESC LIMIT 5",
            "SELECT name, age FROM users ORDER BY age DESC LIMIT 5",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
        }

        Ok(())
    }

    #[test]
    fn test_complex_queries() -> Result<(), Box<dyn Error>> {
        let mut ctx = TestContext::new("test_complex_queries");
        ctx.setup_tables()?;

        let test_cases = vec![
            // Complex join with aggregation
            "SELECT
                u.name,
                COUNT(o.id) as order_count,
                SUM(o.amount) as total_spent
             FROM users u
             LEFT JOIN orders o ON u.id = o.user_id
             GROUP BY u.id, u.name
             HAVING COUNT(o.id) > 0
             ORDER BY total_spent DESC
             LIMIT 10",

            // Multiple joins with filtering
            "SELECT
                u.name,
                p.name as product_name,
                oi.quantity,
                o.amount
             FROM users u
             JOIN orders o ON u.id = o.user_id
             JOIN order_items oi ON o.id = oi.order_id
             JOIN products p ON p.id = oi.product_id
             WHERE o.status = 'completed'
             AND p.category = 'electronics'",

            // Subquery in WHERE clause
            "SELECT name, age
             FROM users
             WHERE id IN (
                 SELECT user_id
                 FROM orders
                 GROUP BY user_id
                 HAVING SUM(amount) > 1000
             )",
        ];

        for sql in test_cases {
            info!("Testing SQL: {}", sql);
            let plan = ctx.planner.explain(sql)?;
            assert!(plan.contains("→"), "Plan should use arrow for hierarchy");
        }

        Ok(())
    }

    #[test]
    fn test_error_handling() {
        let mut ctx = TestContext::new("test_error_handling");
        ctx.setup_tables().unwrap();

        let error_cases = vec![
            // Invalid table
            ("SELECT * FROM nonexistent_table", "not found in catalog"),

            // Invalid column
            ("SELECT nonexistent FROM users", "not found in schema"),

            // Invalid join condition
            ("SELECT * FROM users JOIN orders ON invalid = invalid", "not found in schema"),

            // Invalid group by
            ("SELECT age FROM users GROUP BY nonexistent", "not found in schema"),

            // Invalid having clause
            ("SELECT age FROM users GROUP BY age HAVING invalid > 0", "not found in schema"),

            // Type mismatch in condition
            ("SELECT * FROM users WHERE age = 'invalid'", "type mismatch"),
        ];

        for (sql, expected_error) in error_cases {
            info!("Testing error case: {}", sql);
            let result = ctx.planner.explain(sql);
            assert!(result.is_err(), "Query should fail: {}", sql);
            let error = result.unwrap_err();
            assert!(
                error.to_string().to_lowercase().contains(&expected_error.to_lowercase()),
                "Error '{}' should contain '{}' for query: {}",
                error,
                expected_error,
                sql
            );
        }
    }
}
