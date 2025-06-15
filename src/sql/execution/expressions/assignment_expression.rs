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
        self.value_expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
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
            return Err(ExpressionError::InvalidColumnIndex(
                self.target_column_index,
            ));
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

    #[test]
    fn test_assignment_expression_varchar_assignment() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let target_column = schema.get_column(1).unwrap().clone(); // name column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("Jane".to_string()),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let assignment = AssignmentExpression::new(1, target_column.clone(), value_expr);

        let result = assignment.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new("Jane".to_string()));
        assert_eq!(assignment.to_string(), "name = Jane");
    }

    #[test]
    fn test_assignment_expression_id_assignment() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();
        let target_column = schema.get_column(0).unwrap().clone(); // id column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(999),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(0, target_column.clone(), value_expr);

        let result = assignment.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(999));
        assert_eq!(assignment.get_target_column_index(), 0);
        assert_eq!(assignment.get_target_column().get_name(), "id");
    }

    #[test]
    fn test_assignment_expression_get_return_type() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        let return_type = assignment.get_return_type();
        assert_eq!(return_type.get_name(), "age");
        assert_eq!(return_type.get_type(), TypeId::Integer);
    }

    #[test]
    fn test_assignment_expression_get_child_at() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr.clone());
        
        assert_eq!(assignment.get_children().len(), 1);
    }

    #[test]
    #[should_panic(expected = "index out of bounds")]
    fn test_assignment_expression_get_child_at_invalid_index() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        // This should panic as there's only one child (index 0)
        assignment.get_child_at(1);
    }

    #[test]
    fn test_assignment_expression_clone_with_children() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr.clone());

        let new_value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(40),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let cloned = assignment.clone_with_children(vec![new_value_expr]);

        // Verify the cloned expression is different
        if let Expression::Assignment(cloned_assignment) = cloned.as_ref() {
            assert_eq!(cloned_assignment.get_target_column_index(), 2);
            assert_eq!(cloned_assignment.get_target_column().get_name(), "age");
        } else {
            panic!("Expected Assignment expression");
        }
    }

    #[test]
    #[should_panic(expected = "AssignmentExpression requires exactly one child")]
    fn test_assignment_expression_clone_with_invalid_children_count() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        // This should panic as we're providing 0 children instead of 1
        assignment.clone_with_children(vec![]);
    }

    #[test]
    #[should_panic(expected = "AssignmentExpression requires exactly one child")]
    fn test_assignment_expression_clone_with_too_many_children() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr.clone());

        // This should panic as we're providing 2 children instead of 1
        assignment.clone_with_children(vec![value_expr.clone(), value_expr]);
    }

    #[test]
    fn test_assignment_expression_validation_success() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        // Validation should succeed for valid column index
        assert!(assignment.validate(&schema).is_ok());
    }

    #[test]
    fn test_assignment_expression_validation_invalid_column_index() {
        let schema = create_test_schema();
        let target_column = Column::new("invalid", TypeId::Integer);
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        // Using column index 5 which is out of bounds (schema has 3 columns: 0, 1, 2)
        let assignment = AssignmentExpression::new(5, target_column, value_expr);

        let result = assignment.validate(&schema);
        assert!(result.is_err());
        if let Err(ExpressionError::InvalidColumnIndex(idx)) = result {
            assert_eq!(idx, 5);
        } else {
            panic!("Expected InvalidColumnIndex error");
        }
    }

    #[test]
    fn test_assignment_expression_evaluate_join() {
        let left_schema = create_test_schema();
        let right_schema = Schema::new(vec![
            Column::new("dept_id", TypeId::Integer),
            Column::new("dept_name", TypeId::VarChar),
        ]);

        let left_tuple = create_test_tuple();
        let right_values = vec![Value::new(100), Value::new("Engineering".to_string())];
        let right_tuple = Tuple::new(&right_values, &right_schema, RID::new(1, 0));

        let target_column = left_schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(35),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        let result = assignment
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();
        assert_eq!(result, Value::new(35));
    }

    #[test]
    fn test_assignment_expression_equality() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment1 = AssignmentExpression::new(2, target_column.clone(), value_expr.clone());
        let assignment2 = AssignmentExpression::new(2, target_column.clone(), value_expr.clone());

        assert_eq!(assignment1, assignment2);
    }

    #[test]
    fn test_assignment_expression_inequality_different_index() {
        let schema = create_test_schema();
        let target_column1 = schema.get_column(1).unwrap().clone(); // name column
        let target_column2 = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment1 = AssignmentExpression::new(1, target_column1, value_expr.clone());
        let assignment2 = AssignmentExpression::new(2, target_column2, value_expr);

        assert_ne!(assignment1, assignment2);
    }

    #[test]
    fn test_assignment_expression_debug_format() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);

        // Test that debug formatting works (doesn't panic)
        let debug_str = format!("{:?}", assignment);
        assert!(debug_str.contains("AssignmentExpression"));
    }

    #[test]
    fn test_assignment_expression_clone() {
        let schema = create_test_schema();
        let target_column = schema.get_column(2).unwrap().clone(); // age column
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(30),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let assignment = AssignmentExpression::new(2, target_column.clone(), value_expr);
        let cloned = assignment.clone();

        assert_eq!(assignment, cloned);
        assert_eq!(
            assignment.get_target_column_index(),
            cloned.get_target_column_index()
        );
        assert_eq!(
            assignment.get_target_column().get_name(),
            cloned.get_target_column().get_name()
        );
    }
}
