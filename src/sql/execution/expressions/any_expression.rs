use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use sqlparser::ast::BinaryOperator;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Expression that evaluates a comparison between a value and a list/subquery using ANY/SOME semantics
/// e.g. x > ANY(1,2,3) or x = SOME(SELECT y FROM t)
#[derive(Debug, Clone, PartialEq)]
pub struct AnyExpression {
    left: Arc<Expression>,
    right: Arc<Expression>,
    compare_op: BinaryOperator,
    is_some: bool, // true if SOME, false if ANY (functionally identical)
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl AnyExpression {
    pub fn new(
        left: Arc<Expression>,
        right: Arc<Expression>,
        compare_op: BinaryOperator,
        is_some: bool,
    ) -> Self {
        let children = vec![left.clone(), right.clone()];
        Self {
            left,
            right,
            compare_op,
            is_some,
            return_type: Column::new("any", TypeId::Boolean),
            children,
        }
    }

    fn compare_values(&self, left: &Value, right: &Value) -> Result<CmpBool, ExpressionError> {
        match self.compare_op {
            BinaryOperator::Eq => Ok(left.compare_equals(right)),
            BinaryOperator::NotEq => Ok(left.compare_not_equals(right)),
            BinaryOperator::Gt => Ok(left.compare_greater_than(right)),
            BinaryOperator::GtEq => Ok(left.compare_greater_than_equals(right)),
            BinaryOperator::Lt => Ok(left.compare_less_than(right)),
            BinaryOperator::LtEq => Ok(left.compare_less_than_equals(right)),
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported operator for ANY/SOME: {:?}",
                self.compare_op
            ))),
        }
    }
}

impl ExpressionOps for AnyExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let left_val = self.left.evaluate(tuple, schema)?;
        let right_val = self.right.evaluate(tuple, schema)?;

        // For ANY/SOME, we need the right side to be a vector
        let values = match right_val.get_val() {
            Val::Vector(v) => v,
            _ => {
                return Err(ExpressionError::InvalidOperation(
                    "Right side of ANY/SOME must be a vector or subquery result".to_string(),
                ));
            },
        };

        // ANY/SOME is true if the comparison is true for at least one value
        let mut found_true = false;
        let mut found_null = false;

        for val in values {
            match self.compare_values(&left_val, val)? {
                CmpBool::CmpTrue => {
                    found_true = true;
                    break;
                },
                CmpBool::CmpNull => found_null = true,
                CmpBool::CmpFalse => continue,
            }
        }

        // Return true if we found a match, NULL if we only found NULLs, false otherwise
        if found_true {
            Ok(Value::new(true))
        } else if found_null {
            Ok(Value::new(Val::Null))
        } else {
            Ok(Value::new(false))
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let left_val =
            self.left
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let right_val =
            self.right
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Similar logic to evaluate()
        let values = match right_val.get_val() {
            Val::Vector(v) => v,
            _ => {
                return Err(ExpressionError::InvalidOperation(
                    "Right side of ANY/SOME must be a vector or subquery result".to_string(),
                ));
            },
        };

        let mut found_true = false;
        let mut found_null = false;

        for val in values {
            match self.compare_values(&left_val, val)? {
                CmpBool::CmpTrue => {
                    found_true = true;
                    break;
                },
                CmpBool::CmpNull => found_null = true,
                CmpBool::CmpFalse => continue,
            }
        }

        if found_true {
            Ok(Value::new(true))
        } else if found_null {
            Ok(Value::new(Val::Null))
        } else {
            Ok(Value::new(false))
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 2 {
            panic!("AnyExpression requires exactly 2 children");
        }
        Arc::new(Expression::Any(AnyExpression::new(
            children[0].clone(),
            children[1].clone(),
            self.compare_op.clone(),
            self.is_some,
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate children first
        self.left.validate(schema)?;
        self.right.validate(schema)?;

        // Get the type of right expression (we only need this for now)
        let right_type = self.right.get_return_type().get_type();

        // Right side must be a vector type for ANY/SOME operations
        if right_type != TypeId::Vector {
            return Err(ExpressionError::InvalidOperation(format!(
                "Right side of ANY/SOME must be a vector, got {:?}",
                right_type
            )));
        }

        // Validate that the comparison operator is supported
        match self.compare_op {
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::GtEq
            | BinaryOperator::Lt
            | BinaryOperator::LtEq => Ok(()),
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported operator for ANY/SOME: {:?}",
                self.compare_op
            ))),
        }
    }
}

impl Display for AnyExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} {} {}({})",
            self.left,
            self.compare_op,
            if self.is_some { "SOME" } else { "ANY" },
            self.right
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use sqlparser::ast::BinaryOperator;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
        ])
    }

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = create_test_schema();
        let values = vec![
            Value::new(1),      // id
            Value::new("John"), // name
            Value::new(25),     // age
        ];
        let tuple = Tuple::new(&values, &schema, RID::new(0, 0));
        (tuple, schema)
    }

    #[test]
    fn test_any_expression_basic() {
        let (tuple, schema) = create_test_tuple();

        // Create an ANY expression: age > ANY(20, 30, 40)
        let left = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        let vector_values = vec![Value::new(20), Value::new(30), Value::new(40)];
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vector_values),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AnyExpression::new(
            left,
            right,
            BinaryOperator::Gt,
            false, // ANY
        );

        // Test evaluation (25 > ANY(20, 30, 40) should be true)
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_any_expression_no_match() {
        let (tuple, schema) = create_test_tuple();

        // Create an ANY expression: age > ANY(30, 40, 50)
        let left = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        let vector_values = vec![Value::new(30), Value::new(40), Value::new(50)];
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vector_values),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AnyExpression::new(
            left,
            right,
            BinaryOperator::Gt,
            false, // ANY
        );

        // Test evaluation (25 > ANY(30, 40, 50) should be false)
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));
    }

    #[test]
    fn test_any_expression_with_null() {
        let (tuple, schema) = create_test_tuple();

        // Create an ANY expression: age > ANY(30, NULL, 50)
        let left = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        let vector_values = vec![Value::new(30), Value::new(Val::Null), Value::new(50)];
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vector_values),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AnyExpression::new(
            left,
            right,
            BinaryOperator::Gt,
            false, // ANY
        );

        // Test evaluation (25 > ANY(30, NULL, 50) should be NULL since no true comparison and has NULL)
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert!(matches!(result.get_val(), Val::Null));
    }

    #[test]
    fn test_some_expression() {
        let (tuple, schema) = create_test_tuple();

        // Create a SOME expression: age > SOME(20, 30, 40)
        let left = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        let vector_values = vec![Value::new(20), Value::new(30), Value::new(40)];
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vector_values),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let expr = AnyExpression::new(
            left,
            right,
            BinaryOperator::Gt,
            true, // SOME
        );

        // Test evaluation (25 > SOME(20, 30, 40) should be true)
        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_validate() {
        let schema = create_test_schema();

        let left = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        let vector_values = vec![Value::new(20), Value::new(30)];
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vector_values),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        // Valid expression
        let expr = AnyExpression::new(left.clone(), right.clone(), BinaryOperator::Gt, false);
        assert!(expr.validate(&schema).is_ok());

        // Invalid operator
        let invalid_op_expr =
            AnyExpression::new(left.clone(), right.clone(), BinaryOperator::Plus, false);
        assert!(invalid_op_expr.validate(&schema).is_err());

        // Invalid right type (not a vector)
        let invalid_right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(20),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let invalid_type_expr = AnyExpression::new(left, invalid_right, BinaryOperator::Gt, false);
        assert!(invalid_type_expr.validate(&schema).is_err());
    }

    #[test]
    fn test_display() {
        let left = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2,
            Column::new("age", TypeId::Integer),
            vec![],
        )));

        let vector_values = vec![Value::new(20), Value::new(30)];
        let right = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vector_values),
            Column::new("vector", TypeId::Vector),
            vec![],
        )));

        let any_expr = AnyExpression::new(left.clone(), right.clone(), BinaryOperator::Gt, false);
        assert_eq!(any_expr.to_string(), "age > ANY([20, 30])");

        let some_expr = AnyExpression::new(left, right, BinaryOperator::Gt, true);
        assert_eq!(some_expr.to_string(), "age > SOME([20, 30])");
    }
}
