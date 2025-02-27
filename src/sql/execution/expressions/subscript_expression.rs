use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use crate::types_db::type_id::TypeId;

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
}

impl SubscriptExpression {
    pub fn new(expr: Arc<Expression>, subscript: Subscript, return_type: Column) -> Self {
        Self {
            expr,
            subscript,
            return_type,
        }
    }
}

impl ExpressionOps for SubscriptExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;
        
        // Get the vector from the value
        let vec = match value.get_val() {
            Val::Vector(v) => v,
            _ => return Err(ExpressionError::InvalidType(format!(
                "Cannot perform subscript operation on non-vector type: {:?}", 
                value.get_type_id()
            ))),
        };

        match &self.subscript {
            Subscript::Single(idx) => {
                let idx_val = idx.evaluate(tuple, schema)?;
                
                // Convert index to usize
                let index = match idx_val.get_val() {
                    Val::Integer(i) => {
                        if *i < 0 {
                            let len = vec.len() as i32;
                            // Handle negative indices (counting from end)
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: *i as usize,
                                    size: len as usize
                                });
                            }
                            (len + i) as usize
                        } else {
                            *i as usize
                        }
                    },
                    _ => return Err(ExpressionError::InvalidType(
                        "Array index must be an integer".to_string()
                    )),
                };

                // Check bounds
                if index >= vec.len() {
                    return Err(ExpressionError::IndexOutOfBounds {
                        idx: index,
                        size: vec.len()
                    });
                }

                Ok(vec[index].clone())
            },
            
            Subscript::Range { start, end } => {
                let start_idx = match start {
                    Some(start_expr) => {
                        let start_val = start_expr.evaluate(tuple, schema)?;
                        match start_val.get_val() {
                            Val::Integer(i) => {
                                if *i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len {
                                        0
                                    } else {
                                        (len + i) as usize
                                    }
                                } else {
                                    *i as usize
                                }
                            },
                            _ => return Err(ExpressionError::InvalidType(
                                "Range start index must be an integer".to_string()
                            )),
                        }
                    },
                    None => 0,
                };

                let end_idx = match end {
                    Some(end_expr) => {
                        let end_val = end_expr.evaluate(tuple, schema)?;
                        match end_val.get_val() {
                            Val::Integer(i) => {
                                if *i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len {
                                        0
                                    } else {
                                        (len + i) as usize
                                    }
                                } else {
                                    *i as usize
                                }
                            },
                            _ => return Err(ExpressionError::InvalidType(
                                "Range end index must be an integer".to_string()
                            )),
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
            }
        }
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        
        // Get the vector from the value
        let vec = match value.get_val() {
            Val::Vector(v) => v,
            _ => return Err(ExpressionError::InvalidType(format!(
                "Cannot perform subscript operation on non-vector type: {:?}", 
                value.get_type_id()
            ))),
        };

        match &self.subscript {
            Subscript::Single(idx) => {
                let idx_val = idx.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
                
                // Convert index to usize
                let index = match idx_val.get_val() {
                    Val::Integer(i) => {
                        if *i < 0 {
                            let len = vec.len() as i32;
                            if -i > len {
                                return Err(ExpressionError::IndexOutOfBounds {
                                    idx: *i as usize,
                                    size: len as usize
                                });
                            }
                            (len + i) as usize
                        } else {
                            *i as usize
                        }
                    },
                    _ => return Err(ExpressionError::InvalidType(
                        "Array index must be an integer".to_string()
                    )),
                };

                if index >= vec.len() {
                    return Err(ExpressionError::IndexOutOfBounds {
                        idx: index,
                        size: vec.len()
                    });
                }

                Ok(vec[index].clone())
            },
            
            Subscript::Range { start, end } => {
                // Similar logic as evaluate() for range handling
                let start_idx = match start {
                    Some(start_expr) => {
                        let start_val = start_expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
                        match start_val.get_val() {
                            Val::Integer(i) => {
                                if *i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len {
                                        0
                                    } else {
                                        (len + i) as usize
                                    }
                                } else {
                                    *i as usize
                                }
                            },
                            _ => return Err(ExpressionError::InvalidType(
                                "Range start index must be an integer".to_string()
                            )),
                        }
                    },
                    None => 0,
                };

                let end_idx = match end {
                    Some(end_expr) => {
                        let end_val = end_expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
                        match end_val.get_val() {
                            Val::Integer(i) => {
                                if *i < 0 {
                                    let len = vec.len() as i32;
                                    if -i > len {
                                        0
                                    } else {
                                        (len + i) as usize
                                    }
                                } else {
                                    *i as usize
                                }
                            },
                            _ => return Err(ExpressionError::InvalidType(
                                "Range end index must be an integer".to_string()
                            )),
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
            }
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        match child_idx {
            0 => &self.expr,
            1 => match &self.subscript {
                Subscript::Single(idx) => idx,
                Subscript::Range { start, end } => {
                    if let Some(start) = start {
                        start
                    } else if let Some(end) = end {
                        end
                    } else {
                        panic!("Invalid child index")
                    }
                }
            },
            _ => panic!("Invalid child index"),
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        // Return empty vec since we handle children differently
        static EMPTY: Vec<Arc<Expression>> = Vec::new();
        &EMPTY
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        assert!(!children.is_empty(), "Must provide at least one child expression");
        
        let new_expr = children[0].clone();
        let new_subscript = match &self.subscript {
            Subscript::Single(_) => {
                assert_eq!(children.len(), 2, "Expected 2 children for single subscript");
                Subscript::Single(children[1].clone())
            },
            Subscript::Range { start: _, end: _ } => {
                assert!(children.len() <= 3, "Too many children for range subscript");
                Subscript::Range {
                    start: if children.len() > 1 { Some(children[1].clone()) } else { None },
                    end: if children.len() > 2 { Some(children[2].clone()) } else { None },
                }
            }
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
            return Err(ExpressionError::InvalidType(
                format!("Cannot perform subscript operation on non-vector type: {:?}", 
                    self.expr.get_return_type().get_type())
            ));
        }

        // Validate subscript expressions
        match &self.subscript {
            Subscript::Single(idx) => {
                idx.validate(schema)?;
                if idx.get_return_type().get_type() != TypeId::Integer {
                    return Err(ExpressionError::InvalidType(
                        "Array index must be an integer".to_string()
                    ));
                }
            },
            Subscript::Range { start, end } => {
                if let Some(start) = start {
                    start.validate(schema)?;
                    if start.get_return_type().get_type() != TypeId::Integer {
                        return Err(ExpressionError::InvalidType(
                            "Range start index must be an integer".to_string()
                        ));
                    }
                }
                if let Some(end) = end {
                    end.validate(schema)?;
                    if end.get_return_type().get_type() != TypeId::Integer {
                        return Err(ExpressionError::InvalidType(
                            "Range end index must be an integer".to_string()
                        ));
                    }
                }
            }
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
            }
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
        let tuple = Tuple::new(&*vec![], schema.clone(), crate::common::rid::RID::new(0, 0));

        // Test positive index
        let idx_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::Number("2".to_string(), false),
        ).unwrap()));
        
        let subscript_expr = SubscriptExpression::new(
            vec_expr.clone(),
            Subscript::Single(idx_expr),
            Column::new("result", TypeId::Integer),
        );

        let result = subscript_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(3)); // 0-based indexing, so index 2 gives value 3

        // Test negative index
        let neg_idx_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::Number("-1".to_string(), false),
        ).unwrap()));
        
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
        let tuple = Tuple::new(&*vec![], schema.clone(), crate::common::rid::RID::new(0, 0));

        // Test range with both bounds
        let start_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::Number("1".to_string(), false),
        ).unwrap()));
        let end_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::Number("4".to_string(), false),
        ).unwrap()));

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
        let tuple = Tuple::new(&*vec![], schema.clone(), crate::common::rid::RID::new(0, 0));

        let start_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::Number("-3".to_string(), false),
        ).unwrap()));
        let end_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::Number("-1".to_string(), false),
        ).unwrap()));

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
        let tuple = Tuple::new(&*vec![], schema.clone(), crate::common::rid::RID::new(0, 0));

        // Test index out of bounds
        let idx_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::Number("10".to_string(), false),
        ).unwrap()));
        
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
        let invalid_idx_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::SingleQuotedString("not_a_number".to_string()),
        ).unwrap()));
        
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
        let tuple = Tuple::new(&*vec![], schema.clone(), crate::common::rid::RID::new(0, 0));

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
                assert_eq!(v.len(), 5);
                for i in 0..5 {
                    assert_eq!(v[i], Value::new(i as i32 + 1));
                }
            },
            _ => panic!("Expected vector result"),
        }
    }

    #[test]
    fn test_display() {
        let vec_expr = create_test_vector();
        let idx_expr = Arc::new(Expression::Literal(LiteralValueExpression::new(
            sqlparser::ast::Value::Number("1".to_string(), false),
        ).unwrap()));

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
                start: Some(Arc::new(Expression::Literal(LiteralValueExpression::new(
                    sqlparser::ast::Value::Number("1".to_string(), false),
                ).unwrap()))),
                end: Some(Arc::new(Expression::Literal(LiteralValueExpression::new(
                    sqlparser::ast::Value::Number("3".to_string(), false),
                ).unwrap()))),
            },
            Column::new("result", TypeId::Vector),
        );
        assert_eq!(range_subscript.to_string(), "test_vec[1:3]");
    }
} 