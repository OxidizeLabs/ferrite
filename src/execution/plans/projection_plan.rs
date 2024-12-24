use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::Expression;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionNode {
    output_schema: Arc<Schema>,
    expressions: Vec<Expression>,
    child: Box<PlanNode>,
}

impl ProjectionNode {
    pub fn new(
        output_schema: Arc<Schema>,
        expressions: Vec<Expression>,
        child: PlanNode,
    ) -> Self {
        Self {
            output_schema,
            expressions,
            child: Box::new(child),
        }
    }

    /// Get a reference to the expressions used in this projection
    pub fn get_expressions(&self) -> &Vec<Expression> {
        &self.expressions
    }

    /// Get a reference to the child plan
    pub fn get_child(&self) -> &PlanNode {
        &self.child
    }
}

impl AbstractPlanNode for ProjectionNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static CHILDREN: Vec<PlanNode> = Vec::new();
        // We can't return a reference to a newly created Vec, so we maintain
        // the child in a Box and use a static empty Vec for the trait implementation
        &CHILDREN
    }

    fn get_type(&self) -> PlanType {
        PlanType::Projection
    }

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = String::from("Projection [");

        // Add expressions
        for (i, expr) in self.expressions.iter().enumerate() {
            if i > 0 {
                result.push_str(", ");
            }
            result.push_str(&expr.to_string());
        }
        result.push(']');

        // Add schema if requested
        if with_schema {
            result.push_str("\n  Schema: [");
            result.push_str(&self.output_schema.to_string());
            result.push(']');
        }

        result
    }

    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    fn children_to_string(&self, indent: usize) -> String {
        let indent_str = " ".repeat(indent * 2);
        let mut result = format!("{}└─ {}\n", indent_str, self.to_string(true));

        // Add child plan with increased indentation
        result.push_str(&format!(
            "{}",
            match self.child.as_ref() {
                PlanNode::Projection(node) => node.children_to_string(indent + 1),
                PlanNode::Filter(node) => node.children_to_string(indent + 1),
                PlanNode::TableScan(node) => node.children_to_string(indent + 1),
                // Add other plan types as needed
                _ => String::from("Unknown child node type"),
            }
        ));

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalogue::catalogue::TableInfo;
    use crate::catalogue::column::Column;
    use crate::catalogue::schema::Schema;
    use crate::common::logger::initialize_logger;
    use crate::execution::expressions::mock_expression::MockExpression;
    use crate::execution::plans::table_scan_plan::TableScanNode;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::table_heap::TableHeap;
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::sync::Arc;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
            initialize_logger();
            let buffer_pool_size: usize = 5;
            const K: usize = 2;
            let timestamp = Utc::now().format("%Y%m%d%H%M%S%f").to_string();
            let db_file = format!("tests/data/{}_{}.db", test_name, timestamp);
            let db_log_file = format!("tests/data/{}_{}.log", test_name, timestamp);
            let disk_manager = Arc::new(FileDiskManager::new(
                db_file.clone(),
                db_log_file.clone(),
                100,
            ));
            let disk_scheduler =
                Arc::new(RwLock::new(DiskScheduler::new(Arc::clone(&disk_manager))));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                buffer_pool_size,
                disk_scheduler,
                disk_manager.clone(),
                replacer.clone(),
            ));
            Self {
                bpm,
                db_file,
                db_log_file,
            }
        }

        fn cleanup(&self) {
            let _ = std::fs::remove_file(&self.db_file);
            let _ = std::fs::remove_file(&self.db_log_file);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            self.cleanup()
        }
    }

    fn create_mock_table_scan(ctx: &TestContext, name: &str, schema: Schema) -> PlanNode {
        let table_heap = Arc::new(TableHeap::new(ctx.bpm.clone()));
        let table_info = Arc::new(TableInfo::new(
            schema.clone(),
            name.to_string(),
            table_heap,
            1,
        ));
        PlanNode::TableScan(TableScanNode::new(table_info, Arc::from(schema), None))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_mock_expression(name: &str, return_type: TypeId) -> Expression {
        Expression::Mock(MockExpression::new(
            name.to_string(),
            return_type,
        ))
    }

    #[test]
    fn test_projection_node_creation() {
        let ctx = TestContext::new("test_projection_node_creation");
        let input_schema = create_test_schema();

        let output_schema = Arc::new(Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]));

        let expressions = vec![
            create_mock_expression("id", TypeId::Integer),
            create_mock_expression("name", TypeId::VarChar),
        ];

        let child_node = create_mock_table_scan(&ctx, "test_table", input_schema);
        let projection = ProjectionNode::new(output_schema.clone(), expressions, child_node);

        assert_eq!(projection.get_type(), PlanType::Projection);
        assert_eq!(projection.get_expressions().len(), 2);
        assert_eq!(projection.get_output_schema().get_column_count(), 2);

        // Expression verification
        let exprs = projection.get_expressions();
        match exprs[0].clone() {
            Expression::Mock(mock) => {
                assert_eq!(mock.get_name(), "id");
                assert_eq!(mock.get_type(), TypeId::Integer);
            }
            _ => panic!("Expected Mock expression"),
        }
    }

    #[test]
    fn test_projection_node_string_representation() {
        let ctx = TestContext::new("test_projection_node_string_representation");
        let input_schema = create_test_schema();
        let output_schema = Arc::new(Schema::new(vec![Column::new("id", TypeId::Integer)]));

        let expressions = vec![create_mock_expression("id", TypeId::Integer)];

        let child_node = create_mock_table_scan(&ctx, "test_table", input_schema);
        let projection = ProjectionNode::new(output_schema, expressions, child_node);

        let without_schema = projection.to_string(false);
        assert!(without_schema.starts_with("Projection ["));
        assert!(without_schema.contains("MockExpression(id:Integer)"));

        let with_schema = projection.to_string(true);
        assert!(with_schema.contains("Schema: ["));
        assert!(with_schema.contains("id: Integer"));
    }

    #[test]
    fn test_projection_node_with_different_types() {
        let ctx = TestContext::new("test_projection_node_with_different_types");
        let input_schema = create_test_schema();
        let output_schema = Arc::new(Schema::new(vec![
            Column::new("result", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]));

        let expressions = vec![
            create_mock_expression("result", TypeId::Integer),
            create_mock_expression("name", TypeId::VarChar),
        ];

        let child_node = create_mock_table_scan(&ctx, "test_table", input_schema);
        let projection = ProjectionNode::new(output_schema, expressions, child_node);

        let exprs = projection.get_expressions();
        assert_eq!(exprs.len(), 2);

        // Verify first expression
        match exprs[0].clone() {
            Expression::Mock(mock) => {
                assert_eq!(mock.get_type(), TypeId::Integer);
                assert_eq!(mock.get_name(), "result");
            }
            _ => panic!("Expected Mock expression"),
        }

        // Verify second expression
        match exprs[1].clone(){
            Expression::Mock(mock) => {
                assert_eq!(mock.get_type(), TypeId::VarChar);
                assert_eq!(mock.get_name(), "name");
            }
            _ => panic!("Expected Mock expression"),
        }
    }

    #[test]
    fn test_projection_children_string() {
        let ctx = TestContext::new("test_projection_children_string");
        let input_schema = create_test_schema();
        let output_schema = Arc::new(Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]));

        let expressions = vec![
            create_mock_expression("id", TypeId::Integer),
            create_mock_expression("name", TypeId::VarChar),
        ];

        let child_node = create_mock_table_scan(&ctx, "test_table", input_schema);
        let projection = ProjectionNode::new(output_schema, expressions, child_node);

        let children_string = projection.children_to_string(0);

        // Verify string contains mock expressions
        assert!(children_string.contains("MockExpression(id:Integer)"));
        assert!(children_string.contains("MockExpression(name:VarChar)"));

        // Verify tree structure
        let lines: Vec<&str> = children_string.lines().collect();
        assert_eq!(lines.len(), 3); // Projection, Schema, and TableScan lines
        assert!(lines[0].contains("└─ Projection"));
        assert!(lines[1].starts_with("  Schema:"));
        assert!(lines[2].starts_with("  └─"));
    }
}
