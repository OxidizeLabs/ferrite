use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct InExpression {
    expr: Arc<Expression>,
    subquery: Arc<Expression>,
    negated: bool,
    return_type: Column,
}

impl InExpression {
    pub fn new(
        expr: Arc<Expression>,
        subquery: Arc<Expression>,
        negated: bool,
        return_type: Column,
    ) -> Self {
        Self {
            expr,
            subquery,
            negated,
            return_type,
        }
    }
}

impl ExpressionOps for InExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;
        let subquery_result = self.subquery.evaluate(tuple, schema)?;

        // Handle NULL values
        if value.is_null() {
            return Ok(Value::new(Val::Null));
        }

        // Get the subquery results as a vector
        let subquery_values = match subquery_result.get_val() {
            Val::Vector(values) => values,
            // If single value, wrap in vector
            _ => &vec![subquery_result],
        };

        // Check if value exists in subquery results
        let mut found = false;
        let mut has_null = false;

        for sub_value in subquery_values {
            if sub_value.is_null() {
                has_null = true;
                continue;
            }

            match value.compare_equals(&sub_value) {
                CmpBool::CmpTrue => {
                    found = true;
                    break;
                }
                CmpBool::CmpNull => {
                    has_null = true;
                }
                CmpBool::CmpFalse => {}
            }
        }

        // Handle NULL results according to three-valued logic:
        // - If found, return true/false based on negation
        // - If not found but has nulls, return NULL
        // - If not found and no nulls, return false/true based on negation
        let result = if found {
            !self.negated
        } else if has_null {
            return Ok(Value::new(Val::Null));
        } else {
            self.negated
        };

        Ok(Value::new(result))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let value = self
            .expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let subquery_result =
            self.subquery
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Handle NULL values
        if value.is_null() {
            return Ok(Value::new(Val::Null));
        }

        // Get the subquery results as a vector
        let subquery_values = match subquery_result.get_val() {
            Val::Vector(values) => values,
            // If single value, wrap in vector
            _ => &vec![subquery_result],
        };

        // Check if value exists in subquery results
        let mut found = false;
        let mut has_null = false;

        for sub_value in subquery_values {
            if sub_value.is_null() {
                has_null = true;
                continue;
            }

            match value.compare_equals(&sub_value) {
                CmpBool::CmpTrue => {
                    found = true;
                    break;
                }
                CmpBool::CmpNull => {
                    has_null = true;
                }
                CmpBool::CmpFalse => {}
            }
        }

        let result = if found {
            !self.negated
        } else if has_null {
            return Ok(Value::new(Val::Null));
        } else {
            self.negated
        };

        Ok(Value::new(result))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        match child_idx {
            0 => &self.expr,
            1 => &self.subquery,
            _ => panic!("Invalid child index {} for IN expression", child_idx),
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        static EMPTY: Vec<Arc<Expression>> = Vec::new();
        &EMPTY
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 2 {
            panic!("IN expression requires exactly 2 children");
        }
        Arc::new(Expression::In(InExpression::new(
            children[0].clone(),
            children[1].clone(),
            self.negated,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the expression and subquery
        self.expr.validate(schema)?;
        self.subquery.validate(schema)?;

        // Verify the types are comparable
        let expr_type = self.expr.get_return_type().get_type();
        let subquery_type = self.subquery.get_return_type().get_type();

        // Check if types are compatible for comparison
        if !are_types_comparable(expr_type, subquery_type) {
            return Err(ExpressionError::InvalidOperation(format!(
                "Cannot compare values of types {:?} and {:?} in IN expression",
                expr_type, subquery_type
            )));
        }

        Ok(())
    }
}

// Helper function to check if types can be compared
fn are_types_comparable(left: TypeId, right: TypeId) -> bool {
    match (left, right) {
        // Same types are always comparable
        (a, b) if a == b => true,

        // Numeric types can be compared with each other
        (
            TypeId::Integer | TypeId::BigInt | TypeId::Decimal | TypeId::SmallInt | TypeId::TinyInt,
            TypeId::Integer | TypeId::BigInt | TypeId::Decimal | TypeId::SmallInt | TypeId::TinyInt,
        ) => true,

        // String types can be compared with each other
        (TypeId::Char | TypeId::VarChar, TypeId::Char | TypeId::VarChar) => true,

        // All types can be compared with NULL
        (_, TypeId::Invalid) | (TypeId::Invalid, _) => true,

        // Other type combinations are not comparable
        _ => false,
    }
}

impl Display for InExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}IN ({})",
            self.expr,
            if self.negated { "NOT " } else { "" },
            self.subquery
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::value::Val;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
        ])
    }

    fn create_test_tuple(id: i32, name: &str) -> Tuple {
        let values = vec![Value::new(id), Value::new(name)];
        Tuple::new(&values, create_test_schema(), RID::new(0, 0))
    }

    #[test]
    fn test_in_expression_with_values() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(1, "test");

        // Create expression: id IN (1, 2, 3)
        let column_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let values = vec![Value::new(1), Value::new(2), Value::new(3)];
        let list_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(values),
            Column::new("list", TypeId::Vector),
            vec![],
        )));

        let in_expr = InExpression::new(
            column_expr,
            list_expr,
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = in_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_not_in_expression() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(4, "test");

        // Create expression: id NOT IN (1, 2, 3)
        let column_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let values = vec![Value::new(1), Value::new(2), Value::new(3)];
        let list_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(values),
            Column::new("list", TypeId::Vector),
            vec![],
        )));

        let in_expr = InExpression::new(
            column_expr,
            list_expr,
            true, // negated
            Column::new("result", TypeId::Boolean),
        );

        let result = in_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_in_expression_with_null() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(1, "test");

        // Create expression: id IN (NULL, 2, 3)
        let column_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let values = vec![Value::new(Val::Null), Value::new(2), Value::new(3)];
        let list_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(values),
            Column::new("list", TypeId::Vector),
            vec![],
        )));

        let in_expr = InExpression::new(
            column_expr,
            list_expr,
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = in_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(false));
    }

    #[test]
    fn test_null_in_expression() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(1, "test");

        // Create expression: NULL IN (1, 2, 3)
        let null_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(Val::Null),
            Column::new("null", TypeId::Invalid),
            vec![],
        )));

        let values = vec![Value::new(1), Value::new(2), Value::new(3)];
        let list_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(values),
            Column::new("list", TypeId::Vector),
            vec![],
        )));

        let in_expr = InExpression::new(
            null_expr,
            list_expr,
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = in_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(Val::Null));
    }
}
