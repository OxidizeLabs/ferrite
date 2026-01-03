use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum Subscript {
    Single(Arc<Expression>),
    Range {
        start: Option<Arc<Expression>>,
        end: Option<Arc<Expression>>,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub struct SubscriptExpression {
    expr: Arc<Expression>,
    subscript: Subscript,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl SubscriptExpression {
    pub fn new(expr: Arc<Expression>, subscript: Subscript, return_type: Column) -> Self {
        let mut children = vec![expr.clone()];

        match &subscript {
            Subscript::Single(idx) => {
                children.push(idx.clone());
            },
            Subscript::Range { start, end } => {
                if let Some(start_expr) = start {
                    children.push(start_expr.clone());
                }
                if let Some(end_expr) = end {
                    children.push(end_expr.clone());
                }
            },
        }

        Self {
            expr,
            subscript,
            return_type,
            children,
        }
    }
}

impl ExpressionOps for SubscriptExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;

        // Get the vector from the value
        let vec = match value.get_val() {
            Val::Vector(v) => v,
            _ => {
                return Err(ExpressionError::InvalidType(format!(
                    "Cannot perform subscript operation on non-vector type: {:?}",
                    value.get_type_id()
                )));
            },
        };

        match &self.subscript {
            Subscript::Single(idx) => {
                let idx_val = idx.evaluate(tuple, schema)?;

                // Convert index to usize
                let index = match idx_val.get_val() {
                    Val::TinyInt(i) => {
                        let i = *i as i32;
                        if i < 0 {
                            let len = vec.len() as i32;
                            // Handle negative indices (counting from end)
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: i as usize,
                                    size: len as usize,
                                });
                            }
                            (len + i) as usize
                        } else {
                            i as usize
                        }
                    },
                    Val::SmallInt(i) => {
                        let i = *i as i32;
                        if i < 0 {
                            let len = vec.len() as i32;
                            // Handle negative indices (counting from end)
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: i as usize,
                                    size: len as usize,
                                });
                            }
                            (len + i) as usize
                        } else {
                            i as usize
                        }
                    },
                    Val::Integer(i) => {
                        if *i < 0 {
                            let len = vec.len() as i32;
                            // Handle negative indices (counting from end)
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: *i as usize,
                                    size: len as usize,
                                });
                            }
                            (len + i) as usize
                        } else {
                            *i as usize
                        }
                    },
                    Val::BigInt(i) => {
                        let i = *i as i32;
                        if i < 0 {
                            let len = vec.len() as i32;
                            // Handle negative indices (counting from end)
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: i as usize,
                                    size: len as usize,
                                });
                            }
                            (len + i) as usize
                        } else {
                            i as usize
                        }
                    },
                    _ => {
                        return Err(ExpressionError::InvalidType(
                            "Array index must be an integer".to_string(),
                        ));
                    },
                };

                // Check bounds
                if index >= vec.len() {
                    return Err(ExpressionError::IndexOutOfBounds {
                        idx: index,
                        size: vec.len(),
                    });
                }

                Ok(vec[index].clone())
            },

            Subscript::Range { start, end } => {
                let start_idx = match start {
                    Some(start_expr) => {
                        let start_val = start_expr.evaluate(tuple, schema)?;
                        match start_val.get_val() {
                            Val::TinyInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            Val::SmallInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            Val::Integer(i) => {
                                if *i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    *i as usize
                                }
                            },
                            Val::BigInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            _ => {
                                return Err(ExpressionError::InvalidType(
                                    "Range start index must be an integer".to_string(),
                                ));
                            },
                        }
                    },
                    None => 0,
                };

                let end_idx = match end {
                    Some(end_expr) => {
                        let end_val = end_expr.evaluate(tuple, schema)?;
                        match end_val.get_val() {
                            Val::TinyInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            Val::SmallInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            Val::Integer(i) => {
                                if *i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    *i as usize
                                }
                            },
                            Val::BigInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            _ => {
                                return Err(ExpressionError::InvalidType(
                                    "Range end index must be an integer".to_string(),
                                ));
                            },
                        }
                    },
                    None => vec.len(),
                };

                // Create new vector with range
                let result = if start_idx <= end_idx && end_idx <= vec.len() {
                    vec[start_idx..end_idx].to_vec()
                } else {
                    Vec::new()
                };

                Ok(Value::new_vector(result))
            },
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

        // Get the vector from the value
        let vec = match value.get_val() {
            Val::Vector(v) => v,
            _ => {
                return Err(ExpressionError::InvalidType(format!(
                    "Cannot perform subscript operation on non-vector type: {:?}",
                    value.get_type_id()
                )));
            },
        };

        match &self.subscript {
            Subscript::Single(idx) => {
                let idx_val =
                    idx.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

                // Convert index to usize
                let index = match idx_val.get_val() {
                    Val::TinyInt(i) => {
                        let i = *i as i32;
                        if i < 0 {
                            let len = vec.len() as i32;
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: i as usize,
                                    size: len as usize,
                                });
                            }
                            (len + i) as usize
                        } else {
                            i as usize
                        }
                    },
                    Val::SmallInt(i) => {
                        let i = *i as i32;
                        if i < 0 {
                            let len = vec.len() as i32;
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: i as usize,
                                    size: len as usize,
                                });
                            }
                            (len + i) as usize
                        } else {
                            i as usize
                        }
                    },
                    Val::Integer(i) => {
                        if *i < 0 {
                            let len = vec.len() as i32;
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: *i as usize,
                                    size: len as usize,
                                });
                            }
                            (len + i) as usize
                        } else {
                            *i as usize
                        }
                    },
                    Val::BigInt(i) => {
                        let i = *i as i32;
                        if i < 0 {
                            let len = vec.len() as i32;
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: i as usize,
                                    size: len as usize,
                                });
                            }
                            (len + i) as usize
                        } else {
                            i as usize
                        }
                    },
                    _ => {
                        return Err(ExpressionError::InvalidType(
                            "Array index must be an integer".to_string(),
                        ));
                    },
                };

                if index >= vec.len() {
                    return Err(ExpressionError::IndexOutOfBounds {
                        idx: index,
                        size: vec.len(),
                    });
                }

                Ok(vec[index].clone())
            },

            Subscript::Range { start, end } => {
                // Similar logic as evaluate() for range handling
                let start_idx = match start {
                    Some(start_expr) => {
                        let start_val = start_expr.evaluate_join(
                            left_tuple,
                            left_schema,
                            right_tuple,
                            right_schema,
                        )?;
                        match start_val.get_val() {
                            Val::TinyInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            Val::SmallInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            Val::Integer(i) => {
                                if *i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    *i as usize
                                }
                            },
                            Val::BigInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            _ => {
                                return Err(ExpressionError::InvalidType(
                                    "Range start index must be an integer".to_string(),
                                ));
                            },
                        }
                    },
                    None => 0,
                };

                let end_idx = match end {
                    Some(end_expr) => {
                        let end_val = end_expr.evaluate_join(
                            left_tuple,
                            left_schema,
                            right_tuple,
                            right_schema,
                        )?;
                        match end_val.get_val() {
                            Val::TinyInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            Val::SmallInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            Val::Integer(i) => {
                                if *i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    *i as usize
                                }
                            },
                            Val::BigInt(i) => {
                                let i = *i as i32;
                                if i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len { 0 } else { (len + i) as usize }
                                } else {
                                    i as usize
                                }
                            },
                            _ => {
                                return Err(ExpressionError::InvalidType(
                                    "Range end index must be an integer".to_string(),
                                ));
                            },
                        }
                    },
                    None => vec.len(),
                };

                let result = if start_idx <= end_idx && end_idx <= vec.len() {
                    vec[start_idx..end_idx].to_vec()
                } else {
                    Vec::new()
                };

                Ok(Value::new_vector(result))
            },
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if child_idx < self.children.len() {
            &self.children[child_idx]
        } else {
            panic!("Invalid child index: {}", child_idx)
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        assert!(
            !children.is_empty(),
            "Must provide at least one child expression"
        );

        let new_expr = children[0].clone();
        let new_subscript = match &self.subscript {
            Subscript::Single(_) => {
                assert_eq!(
                    children.len(),
                    2,
                    "Expected 2 children for single subscript"
                );
                Subscript::Single(children[1].clone())
            },
            Subscript::Range { start: _, end: _ } => {
                assert!(children.len() <= 3, "Too many children for range subscript");
                Subscript::Range {
                    start: if children.len() > 1 {
                        Some(children[1].clone())
                    } else {
                        None
                    },
                    end: if children.len() > 2 {
                        Some(children[2].clone())
                    } else {
                        None
                    },
                }
            },
        };

        Arc::new(Expression::Subscript(SubscriptExpression::new(
            new_expr,
            new_subscript,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate base expression
        self.expr.validate(schema)?;

        // Validate that base expression returns a vector
        if self.expr.get_return_type().get_type() != TypeId::Vector {
            return Err(ExpressionError::InvalidType(format!(
                "Cannot perform subscript operation on non-vector type: {:?}",
                self.expr.get_return_type().get_type()
            )));
        }

        // Validate subscript expressions
        match &self.subscript {
            Subscript::Single(idx) => {
                idx.validate(schema)?;
                if idx.get_return_type().get_type() != TypeId::Integer {
                    return Err(ExpressionError::InvalidType(
                        "Array index must be an integer".to_string(),
                    ));
                }
            },
            Subscript::Range { start, end } => {
                if let Some(start) = start {
                    start.validate(schema)?;
                    if start.get_return_type().get_type() != TypeId::Integer {
                        return Err(ExpressionError::InvalidType(
                            "Range start index must be an integer".to_string(),
                        ));
                    }
                }
                if let Some(end) = end {
                    end.validate(schema)?;
                    if end.get_return_type().get_type() != TypeId::Integer {
                        return Err(ExpressionError::InvalidType(
                            "Range end index must be an integer".to_string(),
                        ));
                    }
                }
            },
        }

        Ok(())
    }
}

impl Display for SubscriptExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // For constant expressions containing vectors, display the column name
        if let Expression::Constant(const_expr) = &*self.expr {
            write!(f, "{}", const_expr.get_return_type().get_name())?;
        } else {
            write!(f, "{}", self.expr)?;
        }
        match &self.subscript {
            Subscript::Single(idx) => write!(f, "[{}]", idx),
            Subscript::Range { start, end } => {
                write!(f, "[")?;
                if let Some(start) = start {
                    write!(f, "{}", start)?;
                }
                write!(f, ":")?;
                if let Some(end) = end {
                    write!(f, "{}", end)?;
                }
                write!(f, "]")
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::sql::execution::expressions::literal_value_expression::LiteralValueExpression;

    fn create_test_vector() -> Arc<Expression> {
        let vec = Value::new_vector(vec![
            Value::new(1),
            Value::new(2),
            Value::new(3),
            Value::new(4),
            Value::new(5),
        ]);
        Arc::new(Expression::Constant(ConstantExpression::new(
            vec,
            Column::new("test_vec", TypeId::Vector),
            vec![],
        )))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![])
    }

    #[test]
    fn test_single_index_access() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test positive index
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("2".to_string(), false))
                .unwrap(),
        ));

        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );

        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(3)); // 0-based indexing, so index 2 gives value 3

        // Test negative index
        let neg_idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-1".to_string(), false))
                .unwrap(),
        ));

        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(neg_idx_expr),
            Column::new("result", TypeId::Integer),
        );

        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(5)); // -1 gives last element
    }

    #[test]
    fn test_range_access() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test range with both bounds
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("1".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("4".to_string(), false))
                .unwrap(),
        ));

        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );

        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 3);
                assert_eq!(v[0], Value::new(2));
                assert_eq!(v[1], Value::new(3));
                assert_eq!(v[2], Value::new(4));
            },
            _ => panic!("Expected vector result"),
        }
    }

    #[test]
    fn test_range_with_negative_indices() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-3".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-1".to_string(), false))
                .unwrap(),
        ));

        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );

        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 2);
                assert_eq!(v[0], Value::new(3));
                assert_eq!(v[1], Value::new(4));
            },
            _ => panic!("Expected vector result"),
        }
    }

    #[test]
    fn test_error_cases() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test index out of bounds
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("10".to_string(), false))
                .unwrap(),
        ));

        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );

        assert!(matches!(
            subscript_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::IndexOutOfBounds { .. })
        ));

        // Test invalid index type
        let invalid_idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::SingleQuotedString(
                "not_a_number".to_string(),
            ))
            .unwrap(),
        ));

        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(invalid_idx_expr),
            Column::new("result", TypeId::Integer),
        );

        assert!(matches!(
            subscript_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::InvalidType(_))
        ));
    }

    #[test]
    fn test_empty_ranges() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test range with no bounds (full slice)
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: None,
                end: None,
            },
            Column::new("result", TypeId::Vector),
        );

        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert!(v.len() == 5);
                for (i, item) in v.iter().enumerate().take(5) {
                    assert_eq!(*item, Value::new(i as i32 + 1));
                }
            },
            _ => panic!("Expected vector result"),
        }
    }

    #[test]
    fn test_display() {
        let vec_expr = create_test_vector();
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("1".to_string(), false))
                .unwrap(),
        ));

        // Test single index display
        let single_subscript = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        assert_eq!(single_subscript.to_string(), "test_vec[1]");

        // Test range display
        let range_subscript = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(Arc::new(Expression::Literal(
                    LiteralValueExpression::new(sqlparser::ast::Value::Number(
                        "1".to_string(),
                        false,
                    ))
                    .unwrap(),
                ))),
                end: Some(Arc::new(Expression::Literal(
                    LiteralValueExpression::new(sqlparser::ast::Value::Number(
                        "3".to_string(),
                        false,
                    ))
                    .unwrap(),
                ))),
            },
            Column::new("result", TypeId::Vector),
        );
        assert_eq!(range_subscript.to_string(), "test_vec[1:3]");
    }

    #[test]
    fn test_single_index_edge_cases() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test index 0 (first element)
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("0".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(1)); // First element

        // Test last valid index (4 for 5-element vector)
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("4".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(5)); // Last element

        // Test negative index -5 (first element via negative indexing)
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-5".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(1)); // First element via negative index
    }

    #[test]
    fn test_different_integer_types() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test with TinyInt (value that fits in i8)
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("2".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(3));

        // Test with SmallInt (value that requires i16)
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("200".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        // This should result in IndexOutOfBounds since 200 > 4
        assert!(matches!(
            subscript_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::IndexOutOfBounds { .. })
        ));

        // Test with large value that requires i32
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("50000".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        // This should result in IndexOutOfBounds
        assert!(matches!(
            subscript_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::IndexOutOfBounds { .. })
        ));
    }

    #[test]
    fn test_range_edge_cases() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test range with only start bound
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("2".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: None,
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 3); // Elements 2, 3, 4 (indices 2, 3, 4)
                assert_eq!(v[0], Value::new(3));
                assert_eq!(v[1], Value::new(4));
                assert_eq!(v[2], Value::new(5));
            },
            _ => panic!("Expected vector result"),
        }

        // Test range with only end bound
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("3".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: None,
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 3); // Elements 0, 1, 2 (indices 0, 1, 2)
                assert_eq!(v[0], Value::new(1));
                assert_eq!(v[1], Value::new(2));
                assert_eq!(v[2], Value::new(3));
            },
            _ => panic!("Expected vector result"),
        }

        // Test empty range (start > end)
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("3".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("1".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 0); // Empty range
            },
            _ => panic!("Expected vector result"),
        }

        // Test range with start = end
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("2".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("2".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 0); // Empty range when start == end
            },
            _ => panic!("Expected vector result"),
        }
    }

    #[test]
    fn test_range_beyond_bounds() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test range that extends beyond vector length
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("3".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("10".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 0); // Empty because end > vector.len()
            },
            _ => panic!("Expected vector result"),
        }

        // Test range starting beyond vector length
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("10".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("15".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 0); // Empty because start > vector.len()
            },
            _ => panic!("Expected vector result"),
        }
    }

    #[test]
    fn test_empty_vector() {
        // Create an empty vector
        let empty_vec = Value::new_vector::<Vec<Value>>(vec![]);
        let vec_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            empty_vec,
            Column::new("empty_vec", TypeId::Vector),
            vec![],
        )));
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test single index on empty vector
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("0".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        assert!(matches!(
            subscript_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::IndexOutOfBounds { .. })
        ));

        // Test range on empty vector
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("0".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("1".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 0); // Empty result
            },
            _ => panic!("Expected vector result"),
        }
    }

    #[test]
    fn test_single_element_vector() {
        // Create a single element vector
        let single_vec = Value::new_vector(vec![Value::new(42)]);
        let vec_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            single_vec,
            Column::new("single_vec", TypeId::Vector),
            vec![],
        )));
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test index 0
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("0".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(42));

        // Test negative index -1
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-1".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(42));

        // Test index 1 (out of bounds)
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("1".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        assert!(matches!(
            subscript_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::IndexOutOfBounds { .. })
        ));
    }

    #[test]
    fn test_negative_index_error_cases() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test negative index beyond vector length (-6 for 5-element vector)
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-6".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        assert!(matches!(
            subscript_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::IndexOutOfBounds { .. })
        ));

        // Test large negative index
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-100".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        assert!(matches!(
            subscript_expr.evaluate(&tuple, &schema),
            Err(ExpressionError::IndexOutOfBounds { .. })
        ));
    }

    #[test]
    fn test_mixed_negative_positive_ranges() {
        let vec_expr = create_test_vector();
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test range from negative to positive index
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-3".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("4".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 2); // Elements at indices 2 and 3
                assert_eq!(v[0], Value::new(3));
                assert_eq!(v[1], Value::new(4));
            },
            _ => panic!("Expected vector result"),
        }

        // Test range from positive to negative index (should be empty)
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("3".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("-1".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 1); // Element at index 3
                assert_eq!(v[0], Value::new(4));
            },
            _ => panic!("Expected vector result"),
        }
    }

    #[test]
    fn test_large_vector() {
        // Create a larger vector to test performance and edge cases
        let large_vec_data: Vec<Value> = (0..1000).map(Value::new).collect();
        let large_vec = Value::new_vector(large_vec_data);
        let vec_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            large_vec,
            Column::new("large_vec", TypeId::Vector),
            vec![],
        )));
        let schema = create_test_schema();
        let tuple = Tuple::new(&[], &schema, crate::common::rid::RID::new(0, 0));

        // Test accessing middle element
        let idx_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("500".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(500));

        // Test large range
        let start_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("100".to_string(), false))
                .unwrap(),
        ));
        let end_expr = Arc::new(Expression::Literal(
            LiteralValueExpression::new(sqlparser::ast::Value::Number("200".to_string(), false))
                .unwrap(),
        ));
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Range {
                start: Some(start_expr),
                end: Some(end_expr),
            },
            Column::new("result", TypeId::Vector),
        );
        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Vector(v) => {
                assert_eq!(v.len(), 100); // 100 elements from index 100 to 199
                assert_eq!(v[0], Value::new(100));
                assert_eq!(v[99], Value::new(199));
            },
            _ => panic!("Expected vector result"),
        }
    }
}
