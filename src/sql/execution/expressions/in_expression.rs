use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum InOperand {
    List(Arc<Expression>),     // For IN (val1, val2, ...)
    Subquery(Arc<Expression>), // For IN (SELECT ...)
    Unnest(Arc<Expression>),   // For IN UNNEST(array_expr)
}

#[derive(Clone, Debug, PartialEq)]
pub struct InExpression {
    expr: Arc<Expression>,
    operand: InOperand,
    negated: bool,
    return_type: Column,
    children: Vec<Arc<Expression>>, // Store all child expressions
}

impl InExpression {
    pub fn new_list(
        expr: Arc<Expression>,
        list: Arc<Expression>,
        negated: bool,
        return_type: Column,
    ) -> Self {
        let children = vec![expr.clone(), list.clone()];
        Self {
            expr,
            operand: InOperand::List(list),
            negated,
            return_type,
            children,
        }
    }

    pub fn new_subquery(
        expr: Arc<Expression>,
        subquery: Arc<Expression>,
        negated: bool,
        return_type: Column,
    ) -> Self {
        let children = vec![expr.clone(), subquery.clone()];
        Self {
            expr,
            operand: InOperand::Subquery(subquery),
            negated,
            return_type,
            children,
        }
    }

    pub fn new_unnest(
        expr: Arc<Expression>,
        array_expr: Arc<Expression>,
        negated: bool,
        return_type: Column,
    ) -> Self {
        let children = vec![expr.clone(), array_expr.clone()];
        Self {
            expr,
            operand: InOperand::Unnest(array_expr),
            negated,
            return_type,
            children,
        }
    }

    fn evaluate_list(&self, value: &Value, list_result: Value) -> Result<Value, ExpressionError> {
        // Get the list values as a vector
        let mut owned_values = Vec::new();
        let list_values = match list_result.get_val() {
            Val::Vector(values) => values,
            // If single value, wrap in vector
            _ => {
                owned_values.push(list_result);
                &owned_values
            }
        };

        self.check_value_in_list(value, list_values)
    }

    fn evaluate_subquery(
        &self,
        value: &Value,
        subquery_result: Value,
    ) -> Result<Value, ExpressionError> {
        // Handle subquery results
        match subquery_result.get_val() {
            Val::Vector(values) => self.check_value_in_list(value, values),
            Val::Null => {
                // If the subquery returns NULL, the IN operator result is also NULL
                Ok(Value::new(Val::Null))
            }
            // If the subquery returns a scalar value, convert it to a single-item list
            _ => self.check_value_in_list(value, &[subquery_result]),
        }
    }

    fn evaluate_unnest(
        &self,
        value: &Value,
        unnest_result: Value,
    ) -> Result<Value, ExpressionError> {
        // Handle UNNEST results - should be a vector
        match unnest_result.get_val() {
            Val::Vector(values) => self.check_value_in_list(value, values),
            _ => Err(ExpressionError::InvalidOperation(
                "UNNEST must return an array".to_string(),
            )),
        }
    }

    fn check_value_in_list(&self, value: &Value, list: &[Value]) -> Result<Value, ExpressionError> {
        // Handle NULL value
        if value.is_null() {
            return Ok(Value::new(Val::Null));
        }

        let mut found = false;
        let mut has_null = false;

        for list_value in list {
            if list_value.is_null() {
                has_null = true;
                continue;
            }

            // Handle type conversion for comparison
            // For example, if comparing Integer with TinyInt
            let comparable_value = if value.get_type_id() != list_value.get_type_id() {
                // Try to convert the list value to the same type as the column value
                match (value.get_type_id(), list_value.get_type_id()) {
                    (TypeId::Integer, TypeId::TinyInt) => {
                        // Convert TinyInt to Integer
                        if let Val::TinyInt(n) = list_value.get_val() {
                            Value::new(*n as i32)
                        } else {
                            list_value.clone()
                        }
                    }
                    (TypeId::Integer, TypeId::SmallInt) => {
                        // Convert SmallInt to Integer
                        if let Val::SmallInt(n) = list_value.get_val() {
                            Value::new(*n as i32)
                        } else {
                            list_value.clone()
                        }
                    }
                    (TypeId::Integer, TypeId::BigInt) => {
                        // Convert BigInt to Integer if it fits
                        if let Val::BigInt(n) = list_value.get_val() {
                            if *n >= i32::MIN as i64 && *n <= i32::MAX as i64 {
                                Value::new(*n as i32)
                            } else {
                                list_value.clone()
                            }
                        } else {
                            list_value.clone()
                        }
                    }
                    (TypeId::Integer, TypeId::Decimal) => {
                        // Convert Decimal to Integer if it's a whole number
                        if let Val::Decimal(n) = list_value.get_val() {
                            if *n == (*n as i32) as f64 {
                                Value::new(*n as i32)
                            } else {
                                list_value.clone()
                            }
                        } else {
                            list_value.clone()
                        }
                    }
                    // Add more type conversions as needed
                    _ => list_value.clone(),
                }
            } else {
                list_value.clone()
            };

            // Compare the values
            match value.compare_equals(&comparable_value) {
                crate::types_db::types::CmpBool::CmpTrue => {
                    found = true;
                    break;
                }
                crate::types_db::types::CmpBool::CmpFalse => {
                    // Continue checking other values
                }
                crate::types_db::types::CmpBool::CmpNull => {
                    has_null = true;
                }
            }
        }

        if found {
            Ok(Value::new(true))
        } else if has_null {
            Ok(Value::new(Val::Null))
        } else {
            Ok(Value::new(false))
        }
    }

    fn negate_result(&self, result: Value) -> Result<Value, ExpressionError> {
        match result.get_val() {
            Val::Boolean(b) => Ok(Value::new(!b)),
            Val::Null => Ok(Value::new(Val::Null)), // NULL NOT IN anything is still NULL
            _ => Err(ExpressionError::InvalidOperation(
                "Cannot negate non-boolean IN expression result".to_string(),
            )),
        }
    }
}

impl ExpressionOps for InExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;

        let result = match &self.operand {
            InOperand::List(list_expr) => {
                let list_result = list_expr.evaluate(tuple, schema)?;
                self.evaluate_list(&value, list_result)
            }
            InOperand::Subquery(subquery_expr) => {
                let subquery_result = subquery_expr.evaluate(tuple, schema)?;
                self.evaluate_subquery(&value, subquery_result)
            }
            InOperand::Unnest(array_expr) => {
                let unnest_result = array_expr.evaluate(tuple, schema)?;
                self.evaluate_unnest(&value, unnest_result)
            }
        }?;

        // Apply negation if this is a NOT IN expression
        if self.negated {
            self.negate_result(result)
        } else {
            Ok(result)
        }
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

        let result = match &self.operand {
            InOperand::List(list_expr) => {
                let list_result =
                    list_expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
                self.evaluate_list(&value, list_result)
            }
            InOperand::Subquery(subquery_expr) => {
                let subquery_result = subquery_expr.evaluate_join(
                    left_tuple,
                    left_schema,
                    right_tuple,
                    right_schema,
                )?;
                self.evaluate_subquery(&value, subquery_result)
            }
            InOperand::Unnest(array_expr) => {
                let unnest_result =
                    array_expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
                self.evaluate_unnest(&value, unnest_result)
            }
        }?;

        // Apply negation if this is a NOT IN expression
        if self.negated {
            self.negate_result(result)
        } else {
            Ok(result)
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
            panic!("IN expression requires exactly 2 children");
        }

        let new_in = match &self.operand {
            InOperand::List(_) => InExpression::new_list(
                children[0].clone(),
                children[1].clone(),
                self.negated,
                self.return_type.clone(),
            ),
            InOperand::Subquery(_) => InExpression::new_subquery(
                children[0].clone(),
                children[1].clone(),
                self.negated,
                self.return_type.clone(),
            ),
            InOperand::Unnest(_) => InExpression::new_unnest(
                children[0].clone(),
                children[1].clone(),
                self.negated,
                self.return_type.clone(),
            ),
        };

        Arc::new(Expression::In(new_in))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all children
        for child in &self.children {
            child.validate(schema)?;
        }

        // Verify the types are comparable
        let expr_type = self.expr.get_return_type().get_type();
        let operand_type = match &self.operand {
            InOperand::List(list) => list.get_return_type().get_type(),
            InOperand::Subquery(subquery) => subquery.get_return_type().get_type(),
            InOperand::Unnest(array) => array.get_return_type().get_type(),
        };

        if !are_types_comparable(expr_type, operand_type) {
            return Err(ExpressionError::InvalidOperation(format!(
                "Cannot compare values of types {:?} and {:?} in IN expression",
                expr_type, operand_type
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
            self.children[1]
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
        Tuple::new(&values, &create_test_schema(), RID::new(0, 0))
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

        let in_expr = InExpression::new_list(
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

        let in_expr = InExpression::new_list(
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

        let in_expr = InExpression::new_list(
            column_expr,
            list_expr,
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = in_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(Val::Null));
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

        let in_expr = InExpression::new_list(
            null_expr,
            list_expr,
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = in_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(Val::Null));
    }

    #[test]
    fn test_in_subquery() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(1, "test");

        // Create a mock subquery that returns (1, 2, 3)
        let subquery = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vec![Value::new(1), Value::new(2), Value::new(3)]),
            Column::new("subquery", TypeId::Vector),
            vec![],
        )));

        let column_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let in_expr = InExpression::new_subquery(
            column_expr,
            subquery,
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = in_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_in_unnest() {
        let schema = create_test_schema();
        let tuple = create_test_tuple(1, "test");

        // Create an array expression that contains [1, 2, 3]
        let array_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new_vector(vec![Value::new(1), Value::new(2), Value::new(3)]),
            Column::new("array", TypeId::Vector),
            vec![],
        )));

        let column_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));

        let in_expr = InExpression::new_unnest(
            column_expr,
            array_expr,
            false,
            Column::new("result", TypeId::Boolean),
        );

        let result = in_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }
}
