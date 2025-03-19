use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::{ArrayExpressionError, ExpressionError};
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::types;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct ArrayExpression {
    children: Vec<Arc<Expression>>,
    return_type: Column,
}

impl ArrayExpression {
    pub fn new(elements: Vec<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            children: elements,
            return_type,
        }
    }

    fn evaluate_children<F>(&self, mut eval_func: F) -> Result<Value, ArrayExpressionError>
    where
        F: FnMut(&Expression) -> Result<Value, ExpressionError>,
    {
        // Collect the evaluated values into a Vec<Value>
        let values: Result<Vec<Value>, ArrayExpressionError> = self
            .children
            .iter()
            .map(|expr| {
                eval_func(expr)
                    .map_err(|e| ArrayExpressionError::ChildEvaluationError(e.to_string()))
            })
            .collect();

        // If all values are successfully evaluated, return them wrapped in Val::Vector
        values.map(|v| Value::new(Val::Vector(v)))
    }
}

impl ExpressionOps for ArrayExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        self.evaluate_children(|child| child.evaluate(tuple, schema))
            .map_err(|e| ExpressionError::Array(e))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        self.evaluate_children(|child| {
            child.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
        })
        .map_err(|e| ExpressionError::Array(e))
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
        Arc::new(Expression::Array(ArrayExpression {
            return_type: self.return_type.clone(),
            children,
        }))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all child expressions
        for child in &self.children {
            child.validate(schema)?;

            // Check that all children have compatible types
            let child_type = child.get_return_type().get_type();
            let return_type = self.return_type.get_type();

            // Get the type instance to check coercibility
            let type_instance = types::get_instance(return_type);
            if !type_instance.is_coercible_from(child_type) {
                return Err(ExpressionError::Array(ArrayExpressionError::TypeMismatch(
                    format!(
                        "Array element type {:?} is not compatible with array type {:?}",
                        child_type, return_type
                    ),
                )));
            }
        }

        Ok(())
    }
}

impl Display for ArrayExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ARRAY[{}]",
            self.children
                .iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;

    #[test]
    fn test_array_expression_mixed_types() {
        let children = vec![
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(Val::Integer(1)),
                Column::new("const", TypeId::Integer),
                vec![],
            ))),
            Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(Val::VarLen("hello".to_string())),
                Column::new("const", TypeId::VarChar),
                vec![],
            ))),
        ];
        let expr = Expression::Array(ArrayExpression::new(
            children,
            Column::new("<val>", TypeId::Vector),
        ));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![], schema.clone(), rid);

        let result = expr
            .evaluate(&tuple, &schema)
            .expect("Evaluation should succeed");
        assert_eq!(result.get_type_id(), TypeId::Vector);

        if let Val::Vector(vec) = result.get_val() {
            assert_eq!(vec.len(), 2);
            assert_eq!(vec[0].get_val(), &Val::Integer(1));
            assert_eq!(vec[1].get_val(), &Val::VarLen("hello".to_string()));
        } else {
            panic!("Expected Vector value");
        }
    }

    #[test]
    fn test_empty_array() {
        let expr = Expression::Array(ArrayExpression::new(
            vec![],
            Column::new("<val>", TypeId::Vector),
        ));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![], schema.clone(), rid);

        let result = expr
            .evaluate(&tuple, &schema)
            .expect("Evaluation should succeed");
        assert_eq!(result.get_type_id(), TypeId::Vector);

        if let Val::Vector(vec) = result.get_val() {
            assert_eq!(vec.len(), 0);
        } else {
            panic!("Expected Vector value");
        }
    }

    #[test]
    fn test_nested_arrays() {
        let inner_array = Arc::new(Expression::Array(ArrayExpression::new(
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(Val::Integer(1)),
                Column::new("const", TypeId::Integer),
                vec![],
            )))],
            Column::new("<val>", TypeId::Vector),
        )));

        let outer_array = Expression::Array(ArrayExpression::new(
            vec![inner_array],
            Column::new("<val>", TypeId::Vector),
        ));

        let schema = Schema::new(vec![]);
        let rid = RID::new(0, 0);
        let tuple = Tuple::new(&*vec![], schema.clone(), rid);

        let result = outer_array
            .evaluate(&tuple, &schema)
            .expect("Evaluation should succeed");
        assert_eq!(result.get_type_id(), TypeId::Vector);

        if let Val::Vector(outer_vec) = result.get_val() {
            assert_eq!(outer_vec.len(), 1);
            if let Val::Vector(inner_vec) = outer_vec[0].get_val() {
                assert_eq!(inner_vec.len(), 1);
                assert_eq!(inner_vec[0].get_val(), &Val::Integer(1));
            } else {
                panic!("Expected inner Vector value");
            }
        } else {
            panic!("Expected outer Vector value");
        }
    }
}
