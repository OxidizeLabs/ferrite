use std::fmt;
use std::fmt::{Display, Formatter};
use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::storage::table::tuple::Tuple;
use std::sync::Arc;
use crate::execution::plans::filter_plan::FilterNode;

#[derive(Debug, Clone, PartialEq)]
pub struct InsertNode {
    output_schema: Arc<Schema>,
    table_oid: TableOidT,
    table_name: String,
    tuples: Vec<Tuple>,
    children: Vec<PlanNode>,
}

impl InsertNode {
    pub fn new(
        output_schema: Schema,
        table_oid: TableOidT,
        table_name: String,
        tuples: Vec<Tuple>,
        children: Vec<PlanNode>,
    ) -> Self {
        Self {
            output_schema: Arc::new(output_schema),
            table_oid,
            table_name,
            tuples,
            children
        }
    }

    pub fn get_table_oid(&self) -> TableOidT {
        self.table_oid
    }

    pub fn get_table_name(&self) -> &str {
        &self.table_name
    }

    pub fn get_input_tuples(&self) -> &Vec<Tuple> {
        &self.tuples
    }
}

impl AbstractPlanNode for InsertNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        &self.children
    }

    fn get_type(&self) -> PlanType {
        PlanType::Insert
    }
}

impl Display for InsertNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "→ Insert into {}", self.table_name)?;
        
        if f.alternate() {
            write!(f, "\n   Schema: {}", self.output_schema)?;
            if !self.tuples.is_empty() {
                write!(f, "\n   Values: {} tuples", self.tuples.len())?;
            }
            
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
    use crate::execution::plans::values_plan::ValuesNode;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    mod helpers {
        use super::*;

        pub fn create_test_schema() -> Schema {
            Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
                Column::new("active", TypeId::Boolean),
            ])
        }

        pub fn create_test_tuple(id: i32, name: &str, active: bool, schema: Schema) -> Tuple {
            Tuple::new(
                &*vec![
                    Value::new(id),
                    Value::new(name.to_string()),
                    Value::new(active),
                ],
                schema,
                Default::default(),
            )
        }

        pub fn create_values_plan(schema: Schema) -> PlanNode {
            PlanNode::Values(ValuesNode::new(schema, vec![], vec![PlanNode::Empty]))
        }
    }

    mod construction {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_basic_construction() {
            let schema = create_test_schema();
            let table_oid = 1;
            let table_name = "test_table".to_string();
            let tuples = vec![create_test_tuple(1, "test", true, schema.clone())];
            let child = create_values_plan(schema.clone());

            let insert_node = InsertNode::new(
                schema.clone(),
                table_oid,
                table_name.clone(),
                tuples.clone(),
                vec![child],
            );

            assert_eq!(insert_node.get_table_oid(), table_oid);
            assert_eq!(insert_node.get_table_name(), table_name);
            assert_eq!(insert_node.get_input_tuples(), &tuples);
            assert_eq!(insert_node.get_type(), PlanType::Insert);
        }

        #[test]
        fn test_empty_tuples_construction() {
            let schema = create_test_schema();
            let child = create_values_plan(schema.clone());

            let insert_node =
                InsertNode::new(schema.clone(), 1, "test_table".to_string(), vec![], vec![child]);

            assert!(insert_node.get_input_tuples().is_empty());
        }
    }

    mod schema_handling {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_schema_consistency() {
            let schema = create_test_schema();
            let child = create_values_plan(schema.clone());

            let insert_node =
                InsertNode::new(schema.clone(), 1, "test_table".to_string(), vec![], vec![child]);

            assert_eq!(
                insert_node.get_output_schema().get_columns(),
                schema.get_columns()
            );
            assert_eq!(
                insert_node.get_output_schema().get_column_count(),
                schema.get_column_count()
            );
        }

        #[test]
        fn test_empty_schema() {
            let schema = Schema::new(vec![]);
            let child = create_values_plan(schema.clone());

            let insert_node =
                InsertNode::new(schema.clone(), 1, "test_table".to_string(), vec![], vec![child]);

            assert_eq!(insert_node.get_output_schema().get_column_count(), 0);
        }
    }

    mod child_plan {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_child_access() {
            let schema = create_test_schema();
            let child = create_values_plan(schema.clone());

            let insert_node =
                InsertNode::new(schema, 1, "test_table".to_string(), vec![], vec![child.clone()]);

            assert_eq!(insert_node.get_children()[0], child);
        }

        #[test]
        fn test_children_vec() {
            let schema = create_test_schema();
            let child = create_values_plan(schema.clone());

            let insert_node = InsertNode::new(schema, 1, "test_table".to_string(), vec![], vec![child]);

            // The children vector should be empty as per implementation
            assert!(insert_node.get_children().is_empty());
        }
    }

    mod string_representation {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_to_string_with_schema() {
            let schema = create_test_schema();
            let child = create_values_plan(schema.clone());

            let insert_node = InsertNode::new(schema, 1, "test_table".to_string(), vec![], vec![child]);

            let string_repr = insert_node.to_string();
            assert!(string_repr.contains("InsertNode [table: test_table]"));
            assert!(string_repr.contains("schema:"));
            assert!(string_repr.contains("id"));
            assert!(string_repr.contains("name"));
            assert!(string_repr.contains("active"));
        }

        #[test]
        fn test_to_string_without_schema() {
            let schema = create_test_schema();
            let child = create_values_plan(schema.clone());

            let insert_node = InsertNode::new(schema, 1, "test_table".to_string(), vec![], vec![child]);

            let string_repr = insert_node.to_string();
            assert!(string_repr.contains("InsertNode [table: test_table]"));
            assert!(!string_repr.contains("schema:"));
        }

        #[test]
        fn test_plan_node_to_string() {
            let schema = create_test_schema();
            let child = create_values_plan(schema.clone());

            let insert_node = InsertNode::new(schema, 1, "test_table".to_string(), vec![], vec![child]);

            let string_repr = insert_node.to_string();
            assert!(string_repr.contains("InsertNode [table: test_table]"));
            assert!(string_repr.contains("schema:"));
        }

        #[test]
        fn test_children_to_string() {
            let schema = create_test_schema();
            let child = create_values_plan(schema.clone());

            let insert_node = InsertNode::new(schema, 1, "test_table".to_string(), vec![], vec![child]);

            let string_repr = insert_node.to_string();
            assert!(string_repr.contains("Child:"));
            assert!(string_repr.contains("Values"));
        }
    }

    mod tuple_handling {
        use super::helpers::*;
        use super::*;

        #[test]
        fn test_multiple_tuples() {
            let schema = create_test_schema();
            let tuples = vec![
                create_test_tuple(1, "first", true, schema.clone()),
                create_test_tuple(2, "second", false, schema.clone()),
                create_test_tuple(3, "third", true, schema.clone()),
            ];
            let child = create_values_plan(schema.clone());

            let insert_node =
                InsertNode::new(schema, 1, "test_table".to_string(), tuples.clone(), vec![child]);

            assert_eq!(insert_node.get_input_tuples(), &tuples);
            assert_eq!(insert_node.get_input_tuples().len(), 3);
        }

        #[test]
        fn test_tuple_data_integrity() {
            let schema = create_test_schema();
            let tuple = create_test_tuple(42, "test_name", true, schema.clone());
            let tuples = vec![tuple.clone()];
            let child = create_values_plan(schema.clone());

            let insert_node = InsertNode::new(schema, 1, "test_table".to_string(), tuples, vec![child]);

            let stored_tuple = &insert_node.get_input_tuples()[0];
            assert_eq!(stored_tuple, &tuple);

            // Verify individual values
            assert_eq!(stored_tuple.get_value(0), &Value::new(42));
            assert_eq!(
                stored_tuple.get_value(1),
                &Value::new("test_name".to_string())
            );
            assert_eq!(stored_tuple.get_value(2), &Value::new(true));
        }
    }
}
