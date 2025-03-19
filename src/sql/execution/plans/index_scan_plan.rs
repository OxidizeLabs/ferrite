use crate::catalog::schema::Schema;
use crate::common::config::{IndexOidT, TableOidT};
use crate::sql::execution::expressions::abstract_expression::Expression;
use crate::sql::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct IndexScanNode {
    output_schema: Schema,
    table_name: String,
    table_id: TableOidT,
    index_name: String,
    index_id: IndexOidT,
    predicate_keys: Vec<Arc<Expression>>,
    children: Vec<PlanNode>,
}

impl IndexScanNode {
    pub fn new(
        output_schema: Schema,
        table_name: String,
        table_id: TableOidT,
        index_name: String,
        index_id: IndexOidT,
        predicate_keys: Vec<Arc<Expression>>,
    ) -> Self {
        Self {
            output_schema,
            table_name,
            table_id,
            index_name,
            index_id,
            predicate_keys,
            children: Vec::new(),
        }
    }

    pub fn get_index_name(&self) -> String {
        self.index_name.to_string()
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_index_id(&self) -> IndexOidT {
        self.index_id
    }

    pub fn get_predicate_keys(&self) -> &Vec<Arc<Expression>> {
        &self.predicate_keys
    }
}

impl AbstractPlanNode for IndexScanNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::IndexScan
    }
}

impl Display for IndexScanNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "→ IndexScan: {} using {}",
            self.table_name, self.index_name
        )?;

        if f.alternate() {
            write!(f, "\n   Predicate Keys: [")?;
            for (i, key) in self.predicate_keys.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", key)?;
            }
            write!(f, "]")?;
            write!(f, "\n   Schema: {}", self.output_schema)?;

            // Format children with proper indentation
            for (i, child) in self.children.iter().enumerate() {
                writeln!(f)?;
                write!(f, "    Child {}: {:#}", i + 1, child)?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::types_db::type_id::TypeId;

    fn create_test_schema() -> Schema {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ];
        Schema::new(columns)
    }

    #[test]
    fn test_index_scan_creation() {
        let schema = create_test_schema();
        let table_name = "employees".to_string();
        let table_id = 1;
        let index_name = "emp_id_idx".to_string();
        let index_id = 1;

        // Create predicate key for id column
        let id_col = Column::new("id", TypeId::Integer);
        let pred_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            id_col,
            vec![],
        )));

        let scan_node = IndexScanNode::new(
            schema.clone(),
            table_name,
            table_id,
            index_name,
            index_id,
            vec![pred_key.clone()],
        );

        // Verify the output schema
        let output_schema = scan_node.get_output_schema();
        assert_eq!(output_schema.get_columns().len(), 3);
        assert_eq!(output_schema.get_columns()[0].get_name(), "id");
        assert_eq!(output_schema.get_columns()[1].get_name(), "name");
        assert_eq!(output_schema.get_columns()[2].get_name(), "age");

        // Verify predicate keys
        assert_eq!(scan_node.get_predicate_keys().len(), 1);
        assert_eq!(scan_node.get_predicate_keys()[0], pred_key);
    }

    #[test]
    fn test_index_scan_getters() {
        let schema = create_test_schema();
        let table_name = "employees".to_string();
        let table_id = 1;
        let index_name = "emp_id_idx".to_string();
        let index_id = 1;

        let id_col = Column::new("id", TypeId::Integer);
        let pred_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            id_col,
            vec![],
        )));

        let scan_node = IndexScanNode::new(
            schema.clone(),
            table_name.clone(),
            table_id,
            index_name.clone(),
            index_id,
            vec![pred_key.clone()],
        );

        assert_eq!(scan_node.get_table_name(), "employees");
        assert_eq!(scan_node.get_index_name(), "emp_id_idx");
        assert_eq!(scan_node.get_index_id(), 1);
        assert_eq!(scan_node.get_predicate_keys().len(), 1);
        assert_eq!(scan_node.get_predicate_keys()[0], pred_key);
        assert_eq!(scan_node.get_children().len(), 0);
    }

    #[test]
    fn test_index_scan_display() {
        let schema = create_test_schema();
        let table_name = "employees".to_string();
        let table_id = 1;
        let index_name = "emp_id_idx".to_string();
        let index_id = 1;

        let id_col = Column::new("id", TypeId::Integer);
        let pred_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            id_col,
            vec![],
        )));

        let scan_node = IndexScanNode::new(
            schema,
            table_name,
            table_id,
            index_name,
            index_id,
            vec![pred_key],
        );

        // Test basic display
        assert_eq!(
            scan_node.to_string(),
            "→ IndexScan: employees using emp_id_idx"
        );

        // Test detailed display
        let detailed = format!("{:#}", scan_node);
        assert!(detailed.contains("Predicate Keys:"));
        assert!(detailed.contains("Schema:"));
    }

    #[test]
    fn test_index_scan_multiple_predicates() {
        let schema = create_test_schema();
        let table_name = "employees".to_string();
        let table_id = 1;
        let index_name = "emp_composite_idx".to_string();
        let index_id = 1;

        // Create multiple predicate keys
        let id_col = Column::new("id", TypeId::Integer);
        let age_col = Column::new("age", TypeId::Integer);

        let id_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            id_col,
            vec![],
        )));
        let age_key = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            age_col,
            vec![],
        )));

        let scan_node = IndexScanNode::new(
            schema,
            table_name,
            table_id,
            index_name,
            index_id,
            vec![id_key.clone(), age_key.clone()],
        );

        // Verify multiple predicate keys
        let pred_keys = scan_node.get_predicate_keys();
        assert_eq!(pred_keys.len(), 2);
        assert_eq!(pred_keys[0], id_key);
        assert_eq!(pred_keys[1], age_key);

        // Verify display with multiple keys
        let detailed = format!("{:#}", scan_node);
        assert!(detailed.contains("Predicate Keys:"));
    }

    #[test]
    fn test_plan_type() {
        let schema = create_test_schema();
        let scan_node = IndexScanNode::new(
            schema,
            "employees".to_string(),
            1,
            "emp_id_idx".to_string(),
            1,
            vec![],
        );

        assert_eq!(scan_node.get_type(), PlanType::IndexScan);
    }
}
