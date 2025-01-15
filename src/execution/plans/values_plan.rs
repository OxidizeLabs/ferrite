use std::fmt;
use std::fmt::{Display, Formatter};
use crate::catalog::schema::Schema;
use crate::common::exception::ValuesError;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::Type;
use crate::types_db::value::{Size, Val, Value};
use log::debug;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ValuesNode {
    output_schema: Arc<Schema>,
    rows: Vec<ValueRow>,
    children: Vec<PlanNode>,
}

/// Represents a row of values in a VALUES clause
#[derive(Debug, Clone, PartialEq)]
pub struct ValueRow {
    expressions: Vec<Expression>,
    evaluated_values: Option<Vec<Value>>,
}

impl ValuesNode {
    pub fn new(
        output_schema: Schema,
        expressions: Vec<Vec<Arc<Expression>>>,
        children: Vec<PlanNode>,
    ) -> Self {
        let column_count = output_schema.get_column_count() as usize;
        debug!(
            "Creating ValuesNode with {} rows and schema: {}",
            expressions.len(),
            output_schema
        );

        // Skip validation for empty rows
        if !expressions.is_empty() {
            Self::validate_row_expression_lengths(&expressions, column_count).unwrap();
        }

        // Convert Arc<Expression> to Expression for each row
        let processed_rows: Vec<ValueRow> = expressions
            .into_iter()
            .map(|row_exprs| {
                let exprs = row_exprs
                    .into_iter()
                    .map(|arc_expr| (*arc_expr).clone())
                    .collect();
                ValueRow::new(exprs)
            })
            .collect();

        Self {
            output_schema: Arc::new(output_schema),
            rows: processed_rows,
            children,
        }
    }

    pub fn get_rows(&self) -> &[ValueRow] {
        &self.rows
    }

    pub fn get_row_count(&self) -> usize {
        self.rows.len()
    }

    fn validate_row_expression_lengths(
        expressions: &Vec<Vec<Arc<Expression>>>,
        column_count: usize,
    ) -> Result<(), ValuesError> {
        for row_exprs in expressions.iter() {
            // Skip validation for empty rows
            if !row_exprs.is_empty() && row_exprs.len() != column_count {
                return Err(ValuesError::InvalidValueCount {
                    value_count: row_exprs.len(),
                    column_count,
                });
            }
        }
        Ok(())
    }
}

impl ValueRow {
    pub fn new(expressions: Vec<Expression>) -> Self {
        Self {
            expressions,
            evaluated_values: None,
        }
    }

    pub fn len(&self) -> usize {
        self.expressions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.expressions.is_empty()
    }

    pub fn get_expressions(&self) -> &Vec<Expression> {
        &self.expressions
    }

    /// Evaluates all expressions in the row against a schema
    pub fn evaluate(&mut self, schema: &Schema) -> Result<&[Value], ValuesError> {
        if self.evaluated_values.is_none() {
            let mut values = Vec::with_capacity(self.expressions.len());

            // Create a dummy tuple for evaluation context
            let dummy_values: Vec<Value> = schema
                .get_columns()
                .iter()
                .map(|col| Value {
                    value_: Val::Null,
                    size_: Size::Length(1),
                    manage_data_: false,
                    type_id_: col.get_type(),
                })
                .collect();

            let dummy_tuple = Tuple::new(&dummy_values, schema.clone(), Default::default());

            // Evaluate each expression
            for (idx, expr) in self.expressions.iter().enumerate() {
                let column = schema.get_column(idx).ok_or_else(|| {
                    ValuesError::ValueEvaluationError(format!("Column index {} out of bounds", idx))
                })?;

                let value = expr
                    .evaluate(&dummy_tuple, schema)
                    .map_err(|e| ValuesError::ValueEvaluationError(e.to_string()))?;

                // Verify type compatibility
                if value.get_type_id() != column.get_type() {
                    return Err(ValuesError::TypeMismatch {
                        column_name: column.get_name().to_string(),
                        expected: column.get_type(),
                        actual: value.get_type_id(),
                    });
                }

                values.push(value);
            }

            self.evaluated_values = Some(values);
        }

        Ok(self.evaluated_values.as_ref().unwrap())
    }
}

impl AbstractPlanNode for ValuesNode {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &Vec<PlanNode> {
        static CHILDREN: Vec<PlanNode> = Vec::new();
        &CHILDREN
    }

    fn get_type(&self) -> PlanType {
        PlanType::Values
    }
}

impl Display for ValuesNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.rows.is_empty() {
            write!(f, "→ Values (empty)")?;
        } else {
            write!(f, "→ Values: {} Row(s)", self.rows.len())?;
        }
        
        if f.alternate() {
            write!(f, "\n   Schema: {}", self.output_schema)?;
            
            if self.rows.is_empty() {
                write!(f, "\n   No rows")?;
            } else {
                for (i, row) in self.rows.iter().enumerate() {
                    write!(f, "\n   Row {}: ", i + 1)?;
                    if row.is_empty() {
                        write!(f, "(empty)")?;
                    } else {
                        write!(f, "{}", row)?;
                    }
                }
            }

            // Display children if any
            if !self.children.is_empty() {
                for (i, child) in self.children.iter().enumerate() {
                    write!(f, "\n   Child {}: {:#}", i + 1, child)?;
                }
            }
        }
        
        Ok(())
    }
}

impl Display for ValueRow {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.expressions.is_empty() {
            write!(f, "(empty)")
        } else {
            write!(f, "[")?;
            for (i, expr) in self.expressions.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                match expr {
                    Expression::Constant(c) => write!(f, "{}", c.get_value())?,
                    _ => write!(f, "{}", expr)?,
                }
            }
            write!(f, "]")
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};
    use std::sync::Arc;

    // Test fixtures
    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("active", TypeId::Boolean),
        ])
    }

    fn create_constant_expr(value: Value, name: &str, type_id: TypeId) -> Expression {
        Expression::Constant(ConstantExpression::new(
            value,
            Column::new(name, type_id),
            vec![],
        ))
    }

    mod value_row_tests {
        use super::*;

        #[test]
        fn test_basic_value_row_evaluation() {
            let schema = Schema::new(vec![
                Column::new("id", TypeId::Integer),
                Column::new("name", TypeId::VarChar),
            ]);

            let expressions = vec![
                create_constant_expr(Value::new(42), "id", TypeId::Integer),
                create_constant_expr(Value::new("test"), "name", TypeId::VarChar),
            ];

            let mut row = ValueRow::new(expressions);
            let values = row.evaluate(&schema).unwrap();

            assert_eq!(values.len(), 2);
            assert_eq!(values[0].get_type_id(), TypeId::Integer);
            assert_eq!(values[1].get_type_id(), TypeId::VarChar);
        }

        #[test]
        fn test_empty_value_row() {
            let schema = create_test_schema();
            let mut row = ValueRow::new(vec![]);
            let values = row.evaluate(&schema).unwrap();
            assert!(values.is_empty());
        }

        #[test]
        fn test_type_mismatch() {
            let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
            let expressions = vec![create_constant_expr(
                Value::new("not an integer"),
                "id",
                TypeId::VarChar,
            )];

            let mut row = ValueRow::new(expressions);
            let result = row.evaluate(&schema);

            assert!(matches!(
                result,
                Err(ValuesError::TypeMismatch {
                    column_name,
                    expected: TypeId::Integer,
                    actual: TypeId::VarChar,
                    ..
                }) if column_name == "id"
            ));
        }

        #[test]
        fn test_mixed_value_types() {
            let schema = create_test_schema();
            let expressions = vec![
                create_constant_expr(Value::new(42), "id", TypeId::Integer),
                create_constant_expr(Value::new(Val::VarLen("NULL".to_string())), "name", TypeId::VarChar),
                create_constant_expr(Value::new(true), "active", TypeId::Boolean),
            ];

            let mut row = ValueRow::new(expressions);
            let values = row.evaluate(&schema).unwrap();

            assert_eq!(values.len(), 3);
            assert_eq!(values[0].get_value(), &Val::Integer(42));
            assert_eq!(values[1].get_value(), &Val::VarLen("NULL".to_string()));
            assert_eq!(values[2].get_value(), &Val::Boolean(true));
        }
    }

    mod values_node_tests {
        use super::*;

        #[test]
        fn test_empty_values_node() {
            let schema = create_test_schema();
            let node = ValuesNode::new(schema.clone(), vec![], vec![]);

            assert!(node.get_rows().is_empty());
            assert_eq!(node.get_type(), PlanType::Values);
            assert_eq!(node.get_output_schema(), &schema);
        }

        #[test]
        fn test_single_row_values() {
            let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
            let expressions = vec![vec![Arc::new(create_constant_expr(
                Value::new(1),
                "id",
                TypeId::Integer,
            ))]];

            let node = ValuesNode::new(schema, expressions, vec![]);
            assert_eq!(node.get_rows().len(), 1);
        }

        #[test]
        fn test_mixed_row_values() {
            let schema = create_test_schema();
            let rows = vec![
                vec![], // empty row
                vec![
                    Arc::new(create_constant_expr(Value::new(1), "id", TypeId::Integer)),
                    Arc::new(create_constant_expr(Value::new("test"), "name", TypeId::VarChar)),
                    Arc::new(create_constant_expr(Value::new(true), "active", TypeId::Boolean)),
                ],
                vec![], // another empty row
            ];

            let node = ValuesNode::new(schema, rows, vec![]);
            assert_eq!(node.get_rows().len(), 3);
            assert!(node.get_rows()[0].is_empty());
            assert_eq!(node.get_rows()[1].len(), 3);
            assert!(node.get_rows()[2].is_empty());
        }
    }

    mod display_tests {
        use super::*;

        #[test]
        fn test_empty_values_display() {
            let schema = create_test_schema();
            let node = ValuesNode::new(schema, vec![], vec![]);

            let basic_str = format!("{}", node);
            println!("Basic empty display: {}", basic_str);
            assert!(basic_str.contains("→ Values (empty)"));

            let detailed_str = format!("{:#}", node);
            println!("Detailed empty display: {}", detailed_str);
            assert!(detailed_str.contains("Schema:"));
            assert!(detailed_str.contains("No rows"));
        }

        #[test]
        fn test_empty_rows_display() {
            let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
            let rows = vec![
                vec![], // empty row
                vec![Arc::new(create_constant_expr(Value::new(1), "id", TypeId::Integer))],
                vec![], // another empty row
            ];
            
            let node = ValuesNode::new(schema, rows, vec![]);
            let detailed_str = format!("{:#}", node);
            println!("Mixed rows display: {}", detailed_str);
            
            assert!(detailed_str.contains("Row 1: (empty)"));
            assert!(detailed_str.contains("Row 2: [1]"));
            assert!(detailed_str.contains("Row 3: (empty)"));
        }

        #[test]
        fn test_populated_values_display() {
            let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
            let expressions = vec![vec![Arc::new(create_constant_expr(
                Value::new(42),
                "id",
                TypeId::Integer,
            ))]];

            let node = ValuesNode::new(schema, expressions, vec![]);
            let detailed_str = format!("{:#}", node);
            println!("Populated values display: {}", detailed_str);
            
            assert!(detailed_str.contains("Row 1: [42]"));
        }

        #[test]
        fn test_display_with_children() {
            let schema = create_test_schema();
            let mut node = ValuesNode::new(
                schema.clone(),
                vec![],
                vec![PlanNode::Values(ValuesNode::new(
                    schema,
                    vec![],
                    vec![],
                ))],
            );

            let detailed_str = format!("{:#}", node);
            println!("Values node with children: {}", detailed_str);
            assert!(detailed_str.contains("Child 1:"));
            assert!(detailed_str.contains("Values (empty)"));
        }

        #[test]
        fn test_mixed_row_values() {
            let schema = create_test_schema();
            let rows = vec![
                vec![], // empty row
                vec![
                    Arc::new(create_constant_expr(Value::new(1), "id", TypeId::Integer)),
                    Arc::new(create_constant_expr(Value::new("test"), "name", TypeId::VarChar)),
                    Arc::new(create_constant_expr(Value::new(true), "active", TypeId::Boolean)),
                ],
                vec![], // another empty row
            ];

            let node = ValuesNode::new(schema, rows, vec![]);
            let detailed_str = format!("{:#}", node);
            println!("Mixed values display: {}", detailed_str);
            
            assert!(detailed_str.contains("Row 1: (empty)"));
            assert!(detailed_str.contains("Row 2: [1, test, true]"));
            assert!(detailed_str.contains("Row 3: (empty)"));
        }
    }
}
