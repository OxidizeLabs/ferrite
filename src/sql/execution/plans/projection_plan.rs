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
}

impl ProjectionNode {
    pub fn new(
        output_schema: Schema,
        expressions: Vec<Arc<Expression>>,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema,
            expressions,
            children,
        }
    }

    pub fn get_expressions(&self) -> &Vec<Arc<Expression>> {
        &self.expressions
    }

    pub fn get_child_plan(&self) -> &PlanNode {
        &self.children[0]
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
    use crate::buffer::buffer_pool_manager::BufferPoolManager;
    use crate::buffer::lru_k_replacer::LRUKReplacer;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::concurrency::transaction_manager::TransactionManager;
    use crate::sql::execution::expressions::mock_expression::MockExpression;
    use crate::sql::execution::plans::table_scan_plan::TableScanNode;
    use crate::storage::disk::disk_manager::FileDiskManager;
    use crate::storage::disk::disk_scheduler::DiskScheduler;
    use crate::storage::table::table_heap::{TableHeap, TableInfo};
    use crate::types_db::type_id::TypeId;
    use chrono::Utc;
    use parking_lot::RwLock;
    use std::sync::Arc;

    struct TestContext {
        bpm: Arc<BufferPoolManager>,
        transaction_manager: Arc<TransactionManager>,
        db_file: String,
        db_log_file: String,
    }

    impl TestContext {
        fn new(test_name: &str) -> Self {
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
            let disk_scheduler = Arc::new(RwLock::new(DiskScheduler::new(disk_manager.clone())));
            let replacer = Arc::new(RwLock::new(LRUKReplacer::new(buffer_pool_size, K)));
            let bpm = Arc::new(BufferPoolManager::new(
                buffer_pool_size,
                disk_scheduler,
                disk_manager.clone(),
                replacer,
            ));

            // Create transaction manager
            let transaction_manager = Arc::new(TransactionManager::new());

            Self {
                bpm,
                transaction_manager,
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
        let table_heap = Arc::new(TableHeap::new(
            ctx.bpm.clone(),
            0,
        ));
        let table_info = TableInfo::new(
            schema.clone(),
            name.to_string(),
            table_heap,
            1,
        );
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

    #[test]
    fn test_projection_node_creation() {
        let ctx = TestContext::new("test_projection_node_creation");
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
        let projection = ProjectionNode::new(output_schema.clone(), expressions, child_node);

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

    #[test]
    fn test_projection_node_with_different_types() {
        let ctx = TestContext::new("test_projection_node_with_different_types");
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
        let projection = ProjectionNode::new(output_schema, expressions, child_node);

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
