use crate::catalog::schema::Schema;
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionNode {
    output_schema: Schema,
    expressions: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
    column_mappings: Vec<usize>,
}

impl ProjectionNode {
    pub fn new(
        output_schema: Schema,
        expressions: Vec<Arc<Expression>>,
        column_mappings: Vec<usize>,
    ) -> Self {
        Self {
            output_schema,
            expressions,
            children: vec![],
            column_mappings,
        }
    }

    pub fn with_children(mut self, children: Vec<PlanNode>) -> Self {
        self.children = children;
        self
    }

    pub fn get_expressions(&self) -> &Vec<Arc<Expression>> {
        &self.expressions
    }

    pub fn get_child_plan(&self) -> &PlanNode {
        &self.children[0]
    }

    pub fn get_children_indices(&self) -> &Vec<usize> {
        &self.column_mappings
    }
}

impl Display for ProjectionNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Projection [")?;

        // Add expressions
        for (i, expr) in self.expressions.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", expr)?;
        }
        write!(f, "]")?;

        // Add schema if alternate flag is set
        if f.alternate() {
            writeln!(f)?;
            write!(f, "  Schema: [{}]", self.output_schema)?;

            // Format children with proper indentation
            for (i, child) in self.children.iter().enumerate() {
                writeln!(f)?;
                write!(f, "    Child {}: {:#}", i + 1, child)?;
            }
        }

        Ok(())
    }
}

impl AbstractPlanNode for ProjectionNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Projection
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::buffer_pool_manager_async::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::sql::execution::expressions::mock_expression::MockExpression;
    use crate::sql::execution::plans::table_scan_plan::TableScanNode;
    use crate::storage::table::table_heap::{TableHeap, TableInfo};
    use crate::types_db::type_id::TypeId;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;
    use crate::common::logger::initialize_logger;
    use crate::storage::disk::async_disk_manager::{AsyncDiskManager, DiskManagerConfig};

    struct TestContext {
        bpm: Arc<BufferPoolManager>
    }

    impl TestContext {
        pub async fn new(name: &str) -> Self {
            initialize_logger();
            const BUFFER_POOL_SIZE: usize = 10;
            const K: usize = 2;

            // Create temporary directory
            let temp_dir = TempDir::new().unwrap();
            let db_path = temp_dir
                .path()
                .join(format!("{name}.db"))
                .to_str()
                .unwrap()
                .to_string();
            let log_path = temp_dir
                .path()
                .join(format!("{name}.log"))
                .to_str()
                .unwrap()
                .to_string();

            // Create disk components
            let disk_manager = AsyncDiskManager::new(db_path, log_path, DiskManagerConfig::default()).await;
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(BUFFER_POOL_SIZE, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                BUFFER_POOL_SIZE,
                Arc::from(disk_manager.unwrap()),
                replacer.clone(),
            ).unwrap());

            Self {
                bpm
            }
        }
    }
    fn create_mock_table_scan(ctx: &TestContext, name: &str, schema: Schema) -> PlanNode {
        let table_heap = Arc::new(TableHeap::new(ctx.bpm.clone(), 0));
        let table_info = TableInfo::new(schema.clone(), name.to_string(), table_heap, 1);
        PlanNode::TableScan(TableScanNode::new(table_info, Arc::from(schema), None))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_mock_expression(name: &str, return_type: TypeId) -> Arc<Expression> {
        Arc::new(Expression::Mock(MockExpression::new(
            name.to_string(),
            return_type,
        )))
    }

    #[tokio::test]
    async fn test_projection_node_creation() {
        let ctx = TestContext::new("test_projection_node_creation").await;
        let input_schema = create_test_schema();

        let output_schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let expressions = vec![
            create_mock_expression("id", TypeId::Integer),
            create_mock_expression("name", TypeId::VarChar),
        ];

        let child_node = vec![create_mock_table_scan(&ctx, "test_table", input_schema)];
        let projection = ProjectionNode::new(output_schema.clone(), expressions, vec![0, 1])
            .with_children(child_node);

        assert_eq!(projection.get_type(), PlanType::Projection);
        assert_eq!(projection.get_expressions().len(), 2);
        assert_eq!(projection.get_output_schema().get_column_count(), 2);

        // Expression verification
        let exprs = projection.get_expressions();
        match exprs[0].as_ref() {
            Expression::Mock(mock) => {
                assert_eq!(mock.get_name(), "id");
                assert_eq!(mock.get_type(), TypeId::Integer);
            }
            _ => panic!("Expected Mock expression"),
        }
    }

    #[tokio::test]
    async fn test_projection_node_with_different_types() {
        let ctx = TestContext::new("test_projection_node_with_different_types").await;
        let input_schema = create_test_schema();
        let output_schema = Schema::new(vec![
            Column::new("result", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let expressions = vec![
            create_mock_expression("result", TypeId::Integer),
            create_mock_expression("name", TypeId::VarChar),
        ];

        let child_node = vec![create_mock_table_scan(&ctx, "test_table", input_schema)];
        let projection =
            ProjectionNode::new(output_schema, expressions, vec![0, 1]).with_children(child_node);

        let exprs = projection.get_expressions();
        assert_eq!(exprs.len(), 2);

        // Verify first expression
        match exprs[0].as_ref() {
            Expression::Mock(mock) => {
                assert_eq!(mock.get_type(), TypeId::Integer);
                assert_eq!(mock.get_name(), "result");
            }
            _ => panic!("Expected Mock expression"),
        }

        // Verify second expression
        match exprs[1].as_ref() {
            Expression::Mock(mock) => {
                assert_eq!(mock.get_type(), TypeId::VarChar);
                assert_eq!(mock.get_name(), "name");
            }
            _ => panic!("Expected Mock expression"),
        }
    }
}
