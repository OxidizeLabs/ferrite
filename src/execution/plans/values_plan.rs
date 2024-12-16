// In values_plan.rs
use crate::catalogue::schema::Schema;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::execution::plans::abstract_plan::{AbstractPlanNode, PlanNode, PlanType};
use std::sync::Arc;
use log::debug;
use crate::common::exception::ValuesError;
use crate::storage::table::tuple;
use crate::storage::table::tuple::Tuple;
use crate::types_db::types::Type;
use crate::types_db::value::{Size, Val, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct ValuesNode {
    output_schema: Arc<Schema>,
    rows: Vec<ValueRow>,
    child: Box<PlanNode>,
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
        child: PlanNode,
    ) -> Result<Self, ValuesError> {
        let column_count = output_schema.get_column_count() as usize;
        debug!("Creating ValuesNode with {} rows and schema: {}", expressions.len(), output_schema);

        // Validate row lengths
        Self::validate_row_expression_lengths(&expressions, column_count)?;

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

        Ok(Self {
            output_schema: Arc::new(output_schema),
            rows: processed_rows,
            child: Box::new(child),
        })
    }

    pub fn get_rows(&self) -> &[ValueRow] {
        &self.rows
    }

    pub fn get_row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn get_child(&self) -> &Box<PlanNode> {
        &self.child
    }

    fn validate_row_expression_lengths(
        expressions: &Vec<Vec<Arc<Expression>>>,
        column_count: usize,
    ) -> Result<(), ValuesError> {
        for (idx, row_exprs) in expressions.iter().enumerate() {
            if row_exprs.len() != column_count {
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

                let value = expr.evaluate(&dummy_tuple, schema)
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

    fn to_string(&self, with_schema: bool) -> String {
        let mut result = format!("ValuesNode [{}]", self.rows.len());
        if with_schema {
            result.push_str(&format!(", schema: {}", self.output_schema));
        }
        result
    }

    fn plan_node_to_string(&self) -> String {
        self.to_string(true)
    }

    fn children_to_string(&self, indent: usize) -> String {
        let indent_str = " ".repeat(indent);
        format!("{}Child: {}", indent_str, self.child.plan_node_to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalogue::column::Column;
    use crate::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_value_row_evaluation() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);

        let expressions = vec![
            Expression::Constant(ConstantExpression::new(
                Value::new(42),
                Column::new("id", TypeId::Integer),
                vec![],
            )),
            Expression::Constant(ConstantExpression::new(
                Value::new("test"),
                Column::new("name", TypeId::VarChar),
                vec![],
            )),
        ];

        let mut row = ValueRow::new(expressions);
        let values = row.evaluate(&schema).unwrap();

        assert_eq!(values.len(), 2);
        assert_eq!(values[0].get_type_id(), TypeId::Integer);
        assert_eq!(values[1].get_type_id(), TypeId::VarChar);
    }

    #[test]
    fn test_type_mismatch() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);

        let expressions = vec![Expression::Constant(ConstantExpression::new(
            Value::new("not an integer"),
            Column::new("id", TypeId::VarChar),
            vec![],
        ))];

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
    fn test_values_node_creation() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let expressions = vec![vec![Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("id", TypeId::Integer),
            vec![],
        )))]];

        let result = ValuesNode::new(schema, expressions, PlanNode::Empty);
        assert!(result.is_ok());
    }

    #[test]
    fn test_invalid_value_count() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ]);
        let expressions = vec![vec![Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("id", TypeId::Integer),
            vec![],
        )))]]; // Only one column when schema expects two

        let result = ValuesNode::new(schema, expressions, PlanNode::Empty);
        assert!(matches!(
            result,
            Err(ValuesError::InvalidValueCount {
                value_count: 1,
                column_count: 2,
            })
        ));
    }

    #[test]
    fn test_value_row_empty_evaluation() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let row = ValueRow::new(vec![]);

        assert!(row.is_empty());
        assert_eq!(row.len(), 0);
    }

    #[test]
    fn test_value_row_mixed_values() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("active", TypeId::Boolean),
        ]);

        let expressions = vec![
            Expression::Constant(ConstantExpression::new(
                Value::new(42),
                Column::new("id", TypeId::Integer),
                vec![],
            )),
            Expression::Constant(ConstantExpression::new(
                Value::new(Val::VarLen("NULL".to_string())),
                Column::new("name", TypeId::VarChar),
                vec![],
            )),
            Expression::Constant(ConstantExpression::new(
                Value::new(true),
                Column::new("active", TypeId::Boolean),
                vec![],
            )),
        ];

        let mut row = ValueRow::new(expressions);
        let values = row.evaluate(&schema).unwrap();

        assert_eq!(values.len(), 3);
        assert_eq!(values[0].get_value(), &Val::Integer(42));
        assert_eq!(values[1].get_value(), &Val::VarLen("NULL".to_string()));
        assert_eq!(values[2].get_value(), &Val::Boolean(true));
    }

    #[test]
    fn test_multiple_evaluations() {
        let schema = Schema::new(vec![Column::new("id", TypeId::Integer)]);
        let expressions = vec![Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("id", TypeId::Integer),
            vec![],
        ))];

        let mut row = ValueRow::new(expressions);

        // First evaluation
        let values1 = row.evaluate(&schema).unwrap().to_vec();
        assert_eq!(values1.len(), 1);

        // Second evaluation should return the same cached values
        let values2 = row.evaluate(&schema).unwrap().to_vec();
        assert_eq!(values1, values2);
    }
}