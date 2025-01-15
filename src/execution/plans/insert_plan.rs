use crate::catalog::schema::Schema;
use crate::common::config::TableOidT;
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::storage::table::tuple::Tuple;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

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
            children,
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
        write!(f, "→ Insert [table: {}]", self.table_name)?;

        if f.alternate() {
            write!(f, "\n   Table ID: {}", self.table_oid)?;
            write!(f, "\n   Schema: {}", self.output_schema)?;

            // Format children with proper indentation
            for (i, child) in self.children.iter().enumerate() {
                writeln!(f)?;
                write!(f, "   Child {}: {:#}", i + 1, child)?;
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
        use super::*;

        #[test]
        fn test_children_vec() {
            let schema = helpers::create_test_schema();

            // Create an insert node with no children
            let insert_node = InsertNode::new(
                schema.clone(),
                1,
                "test_table".to_string(),
                vec![],
                vec![], // Empty children vector
            );

            // Verify that a newly created insert node has no children
            assert!(insert_node.get_children().is_empty());
        }

        #[test]
        fn test_with_values_child() {
            let schema = helpers::create_test_schema();

            // Create a values node as child
            let values_node = PlanNode::Values(ValuesNode::new(
                schema.clone(),
                vec![], // empty rows
                vec![], // empty children
            ));

            // Create insert node with the values node as child
            let insert_node = InsertNode::new(
                schema,
                1,
                "test_table".to_string(),
                vec![],
                vec![values_node], // Add values node as child
            );

            // Verify that the insert node has exactly one child
            assert_eq!(insert_node.get_children().len(), 1);

            // Verify that the child is a values node
            match &insert_node.get_children()[0] {
                PlanNode::Values(_) => (),
                _ => panic!("Expected Values node as child"),
            }
        }
    }

    mod string_representation {
        use super::*;

        #[test]
        fn test_children_to_string() {
            let schema = helpers::create_test_schema();

            // Create a values node as child
            let values_node = PlanNode::Values(ValuesNode::new(
                schema.clone(),
                vec![],
                vec![],
            ));

            // Create insert node with the values node as child
            let insert_node = InsertNode::new(
                schema,
                1,
                "test_table".to_string(),
                vec![],
                vec![values_node],
            );

            let string_repr = format!("{:#}", insert_node);
            println!("Insert node string representation: {}", string_repr);

            assert!(string_repr.contains("→ Insert [table: test_table]"));
            assert!(string_repr.contains("Schema:"));
            assert!(string_repr.contains("Child 1:"));
        }

        #[test]
        fn test_to_string_with_schema() {
            let schema = helpers::create_test_schema();
            let insert_node = InsertNode::new(
                schema,
                1,
                "test_table".to_string(),
                vec![],
                vec![],
            );

            let string_repr = format!("{:#}", insert_node);
            println!("Insert node with schema: {}", string_repr);

            assert!(string_repr.contains("→ Insert [table: test_table]"));
            assert!(string_repr.contains("Schema:"));
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
