use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Expression that represents an assignment in an UPDATE statement
/// Encapsulates both the target column and the value expression
#[derive(Debug, Clone, PartialEq)]
pub struct AssignmentExpression {
    /// Index of the target column in the table schema
    target_column_index: usize,
    /// The target column information
    target_column: Column,
    /// The value expression to assign
    value_expr: Arc<Expression>,
    /// Child expressions (just the value expression)
    children: Vec<Arc<Expression>>,
}

impl AssignmentExpression {
    pub fn new(
        target_column_index: usize,
        target_column: Column,
        value_expr: Arc<Expression>,
    ) -> Self {
        let children = vec![value_expr.clone()];
        
        Self {
            target_column_index,
            target_column,
            value_expr,
            children,
        }
    }

    pub fn get_target_column_index(&self) -> usize {
        self.target_column_index
    }

    pub fn get_target_column(&self) -> &Column {
        &self.target_column
    }

    pub fn get_value_expr(&self) -> &Arc<Expression> {
        &self.value_expr
    }
}

impl ExpressionOps for AssignmentExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        // Evaluate the value expression
        self.value_expr.evaluate(tuple, schema)
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Evaluate the value expression in join context
        self.value_expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        // Return the target column type since that's what we're assigning to
        &self.target_column
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 1 {
            panic!("AssignmentExpression requires exactly one child (the value expression)");
        }

        Arc::new(Expression::Assignment(AssignmentExpression::new(
            self.target_column_index,
            self.target_column.clone(),
            children[0].clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate that the target column index is within bounds
        if self.target_column_index >= schema.get_column_count() as usize {
            return Err(ExpressionError::InvalidColumnIndex(self.target_column_index));
        }

        // Validate the value expression
        self.value_expr.validate(schema)?;

        Ok(())
    }
}

impl Display for AssignmentExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{} = {}", self.target_column.get_name(), self.value_expr)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::schema::Schema;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_schema() -> Schema {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ];
        Schema::new(columns)
    }

    fn create_test_tuple() -> Tuple {
        let schema = create_test_schema();
        let values = vec![
            Value::new(1),
            Value::new("John".to_string()),
            Value::new(25),
        ];
        Tuple::new(&values, &schema, RID::new(0, 0))
    }

    #[test]
    fn test_assignment_expression_creation() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        assert_eq!(assignment.get_target_column_index(), 2);
        assert_eq!(assignment.get_target_column().get_name(), "age");
        assert_eq!(assignment.get_children().len(), 1);
    }

    #[test]
    fn test_assignment_expression_evaluation() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        let result = assignment.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(30));
    }

    #[test]
    fn test_assignment_expression_display() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        assert_eq!(assignment.to_string(), "age = 30");
    }
} 