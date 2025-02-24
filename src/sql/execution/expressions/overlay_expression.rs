use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct OverlayExpression {
    expr: Arc<Expression>,
    overlay_what: Arc<Expression>,
    overlay_from: Arc<Expression>,
    overlay_for: Option<Arc<Expression>>,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl OverlayExpression {
    pub fn new(
        expr: Arc<Expression>,
        overlay_what: Arc<Expression>,
        overlay_from: Arc<Expression>,
        overlay_for: Option<Arc<Expression>>,
        return_type: Column,
    ) -> Self {
        // Build children vector
        let mut children = vec![expr.clone(), overlay_what.clone(), overlay_from.clone()];
        if let Some(for_expr) = &overlay_for {
            children.push(for_expr.clone());
        }

        Self {
            expr,
            overlay_what,
            overlay_from,
            overlay_for,
            return_type,
            children,
        }
    }
}

impl ExpressionOps for OverlayExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let base_str = self.expr.evaluate(tuple, schema)?;
        let overlay_str = self.overlay_what.evaluate(tuple, schema)?;
        let from_pos = self.overlay_from.evaluate(tuple, schema)?;
        let for_len = if let Some(for_expr) = &self.overlay_for {
            Some(for_expr.evaluate(tuple, schema)?)
        } else {
            None
        };

        // Extract string values
        let base_text = match base_str.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "OVERLAY base expression must be a string, got {:?}",
                    base_str.get_type_id()
                )))
            }
        };

        let overlay_text = match overlay_str.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "OVERLAY PLACING expression must be a string, got {:?}",
                    overlay_str.get_type_id()
                )))
            }
        };

        // Get the starting position (1-indexed in SQL)
        let start_pos = match from_pos.get_val() {
            Val::Integer(i) => {
                if *i < 1 {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "OVERLAY FROM position must be >= 1, got {}",
                        i
                    )));
                }
                (*i as usize).saturating_sub(1) // Convert to 0-indexed for Rust
            }
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "OVERLAY FROM expression must be an integer, got {:?}",
                    from_pos.get_type_id()
                )))
            }
        };

        // Get the length to replace (if specified)
        let replace_len = match for_len {
            Some(len) => match len.get_val() {
                Val::Integer(i) => {
                    if *i < 0 {
                        return Err(ExpressionError::InvalidOperation(format!(
                            "OVERLAY FOR length must be >= 0, got {}",
                            i
                        )));
                    }
                    *i as usize
                }
                _ => {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "OVERLAY FOR expression must be an integer, got {:?}",
                        len.get_type_id()
                    )))
                }
            },
            None => overlay_text.len(), // Default to length of overlay string if FOR is not specified
        };

        // Perform the overlay operation
        let result = if start_pos >= base_text.len() {
            // If start position is beyond the end of the string, just append the overlay string
            format!("{}{}", base_text, overlay_text)
        } else {
            // Calculate the end position for replacement
            let end_pos = std::cmp::min(start_pos + replace_len, base_text.len());

            // Construct the result string
            format!(
                "{}{}{}",
                &base_text[0..start_pos],
                overlay_text,
                &base_text[end_pos..]
            )
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
        let base_str =
            self.expr
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let overlay_str =
            self.overlay_what
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let from_pos =
            self.overlay_from
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let for_len = if let Some(for_expr) = &self.overlay_for {
            Some(for_expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?)
        } else {
            None
        };

        // Extract string values
        let base_text = match base_str.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "OVERLAY base expression must be a string, got {:?}",
                    base_str.get_type_id()
                )))
            }
        };

        let overlay_text = match overlay_str.get_val() {
            Val::VarLen(s) | Val::ConstLen(s) => s.clone(),
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "OVERLAY PLACING expression must be a string, got {:?}",
                    overlay_str.get_type_id()
                )))
            }
        };

        // Get the starting position (1-indexed in SQL)
        let start_pos = match from_pos.get_val() {
            Val::Integer(i) => {
                if *i < 1 {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "OVERLAY FROM position must be >= 1, got {}",
                        i
                    )));
                }
                (*i as usize).saturating_sub(1) // Convert to 0-indexed for Rust
            }
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "OVERLAY FROM expression must be an integer, got {:?}",
                    from_pos.get_type_id()
                )))
            }
        };

        // Get the length to replace (if specified)
        let replace_len = match for_len {
            Some(len) => match len.get_val() {
                Val::Integer(i) => {
                    if *i < 0 {
                        return Err(ExpressionError::InvalidOperation(format!(
                            "OVERLAY FOR length must be >= 0, got {}",
                            i
                        )));
                    }
                    *i as usize
                }
                _ => {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "OVERLAY FOR expression must be an integer, got {:?}",
                        len.get_type_id()
                    )))
                }
            },
            None => overlay_text.len(), // Default to length of overlay string if FOR is not specified
        };

        // Perform the overlay operation
        let result = if start_pos >= base_text.len() {
            // If start position is beyond the end of the string, just append the overlay string
            format!("{}{}", base_text, overlay_text)
        } else {
            // Calculate the end position for replacement
            let end_pos = std::cmp::min(start_pos + replace_len, base_text.len());

            // Construct the result string
            format!(
                "{}{}{}",
                &base_text[0..start_pos],
                overlay_text,
                &base_text[end_pos..]
            )
        };

        Ok(Value::new(result))
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
        if children.len() < 3 || children.len() > 4 {
            panic!("OverlayExpression requires 3 or 4 children");
        }

        let overlay_for = if children.len() == 4 {
            Some(children[3].clone())
        } else {
            None
        };

        Arc::new(Expression::Overlay(OverlayExpression::new(
            children[0].clone(),
            children[1].clone(),
            children[2].clone(),
            overlay_for,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all child expressions
        for child in &self.children {
            child.validate(schema)?;
        }

        // Check that base expression returns a string
        let base_type = self.expr.get_return_type().get_type();
        if base_type != TypeId::VarChar && base_type != TypeId::Char {
            return Err(ExpressionError::InvalidOperation(format!(
                "OVERLAY base expression must return a string type, got {:?}",
                base_type
            )));
        }

        // Check that overlay_what expression returns a string
        let overlay_type = self.overlay_what.get_return_type().get_type();
        if overlay_type != TypeId::VarChar && overlay_type != TypeId::Char {
            return Err(ExpressionError::InvalidOperation(format!(
                "OVERLAY PLACING expression must return a string type, got {:?}",
                overlay_type
            )));
        }

        // Check that overlay_from expression returns an integer
        let from_type = self.overlay_from.get_return_type().get_type();
        if from_type != TypeId::Integer
            && from_type != TypeId::BigInt
            && from_type != TypeId::SmallInt
            && from_type != TypeId::TinyInt
        {
            return Err(ExpressionError::InvalidOperation(format!(
                "OVERLAY FROM expression must return an integer type, got {:?}",
                from_type
            )));
        }

        // Check that overlay_for expression returns an integer if present
        if let Some(for_expr) = &self.overlay_for {
            let for_type = for_expr.get_return_type().get_type();
            if for_type != TypeId::Integer
                && for_type != TypeId::BigInt
                && for_type != TypeId::SmallInt
                && for_type != TypeId::TinyInt
            {
                return Err(ExpressionError::InvalidOperation(format!(
                    "OVERLAY FOR expression must return an integer type, got {:?}",
                    for_type
                )));
            }
        }

        Ok(())
    }
}

impl Display for OverlayExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "OVERLAY({} PLACING {} FROM {}{})",
            self.expr,
            self.overlay_what,
            self.overlay_from,
            if let Some(for_expr) = &self.overlay_for {
                format!(" FOR {}", for_expr)
            } else {
                String::new()
            }
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    // Helper function to create a constant string expression
    fn create_string_expr(value: &str) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(value),
            Column::new("string_const", TypeId::VarChar),
            vec![],
        )))
    }

    // Helper function to create a constant integer expression
    fn create_int_expr(value: i32) -> Arc<Expression> {
        Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(value),
            Column::new("int_const", TypeId::Integer),
            vec![],
        )))
    }

    #[test]
    fn test_basic_overlay() {
        // Test: OVERLAY('abcdef' PLACING 'xyz' FROM 2)
        // Expected: 'axyzdef'

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "axyzdef");
    }

    #[test]
    fn test_overlay_with_for() {
        // Test: OVERLAY('abcdef' PLACING 'xyz' FROM 2 FOR 3)
        // Expected: 'axyzef'

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            Some(create_int_expr(3)),
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "axyzef");
    }

    #[test]
    fn test_overlay_start_beyond_string() {
        // Test: OVERLAY('abc' PLACING 'xyz' FROM 10)
        // Expected: 'abcxyz'

        let expr = OverlayExpression::new(
            create_string_expr("abc"),
            create_string_expr("xyz"),
            create_int_expr(10),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "abcxyz");
    }

    #[test]
    fn test_overlay_for_zero() {
        // Test: OVERLAY('abcdef' PLACING 'xyz' FROM 2 FOR 0)
        // Expected: 'axyzdef' (insert without replacing)

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            Some(create_int_expr(0)),
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "axyzdef");
    }

    #[test]
    fn test_overlay_for_beyond_string() {
        // Test: OVERLAY('abc' PLACING 'xyz' FROM 2 FOR 10)
        // Expected: 'axyz'

        let expr = OverlayExpression::new(
            create_string_expr("abc"),
            create_string_expr("xyz"),
            create_int_expr(2),
            Some(create_int_expr(10)),
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "axyz");
    }

    #[test]
    fn test_overlay_empty_base() {
        // Test: OVERLAY('' PLACING 'xyz' FROM 1)
        // Expected: 'xyz'

        let expr = OverlayExpression::new(
            create_string_expr(""),
            create_string_expr("xyz"),
            create_int_expr(1),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "xyz");
    }

    #[test]
    fn test_overlay_empty_overlay() {
        // Test: OVERLAY('abcdef' PLACING '' FROM 2)
        // Expected: 'abcdef' (no change when overlay is empty and no FOR)

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr(""),
            create_int_expr(2),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "abcdef");
    }

    #[test]
    fn test_overlay_empty_overlay_with_for() {
        // Test: OVERLAY('abcdef' PLACING '' FROM 2 FOR 3)
        // Expected: 'aef' (remove characters when overlay is empty but FOR is specified)

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr(""),
            create_int_expr(2),
            Some(create_int_expr(3)),
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "aef");
    }

    #[test]
    fn test_overlay_first_position() {
        // Test: OVERLAY('abcdef' PLACING 'xyz' FROM 1)
        // Expected: 'xyzdef'

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(1),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "xyzdef");
    }

    #[test]
    fn test_overlay_last_position() {
        // Test: OVERLAY('abcdef' PLACING 'xyz' FROM 6)
        // Expected: 'abcdexyz'

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(6),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "abcdexyz");
    }

    #[test]
    fn test_overlay_invalid_position() {
        // Test: OVERLAY('abcdef' PLACING 'xyz' FROM 0)
        // Expected: Error (position must be >= 1)

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(0),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be >= 1"));
    }

    #[test]
    fn test_overlay_invalid_for() {
        // Test: OVERLAY('abcdef' PLACING 'xyz' FROM 2 FOR -1)
        // Expected: Error (FOR length must be >= 0)

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            Some(create_int_expr(-1)),
            Column::new("result", TypeId::VarChar),
        );

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], schema.clone(), RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be >= 0"));
    }

    #[test]
    fn test_overlay_display() {
        // Test the Display implementation

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            Some(create_int_expr(3)),
            Column::new("result", TypeId::VarChar),
        );

        assert_eq!(expr.to_string(), "OVERLAY(abcdef PLACING xyz FROM 2 FOR 3)");

        let expr_without_for = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            None,
            Column::new("result", TypeId::VarChar),
        );

        assert_eq!(
            expr_without_for.to_string(),
            "OVERLAY(abcdef PLACING xyz FROM 2)"
        );
    }

    #[test]
    fn test_overlay_children() {
        // Test the children vector

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            Some(create_int_expr(3)),
            Column::new("result", TypeId::VarChar),
        );

        assert_eq!(expr.get_children().len(), 4);
        assert_eq!(expr.get_child_at(0).to_string(), "abcdef");
        assert_eq!(expr.get_child_at(1).to_string(), "xyz");
        assert_eq!(expr.get_child_at(2).to_string(), "2");
        assert_eq!(expr.get_child_at(3).to_string(), "3");

        let expr_without_for = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            None,
            Column::new("result", TypeId::VarChar),
        );

        assert_eq!(expr_without_for.get_children().len(), 3);
    }

    #[test]
    fn test_clone_with_children() {
        // Test cloning with new children

        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            Some(create_int_expr(3)),
            Column::new("result", TypeId::VarChar),
        );

        let new_children = vec![
            create_string_expr("123456"),
            create_string_expr("789"),
            create_int_expr(3),
            create_int_expr(2),
        ];

        let cloned = expr.clone_with_children(new_children);

        if let Expression::Overlay(overlay_expr) = &*cloned {
            assert_eq!(overlay_expr.get_child_at(0).to_string(), "123456");
            assert_eq!(overlay_expr.get_child_at(1).to_string(), "789");
            assert_eq!(overlay_expr.get_child_at(2).to_string(), "3");
            assert_eq!(overlay_expr.get_child_at(3).to_string(), "2");
        } else {
            panic!("Expected Overlay expression");
        }
    }

    #[test]
    #[should_panic(expected = "OverlayExpression requires 3 or 4 children")]
    fn test_clone_with_too_few_children() {
        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let new_children = vec![create_string_expr("123456"), create_string_expr("789")];

        expr.clone_with_children(new_children);
    }

    #[test]
    #[should_panic(expected = "OverlayExpression requires 3 or 4 children")]
    fn test_clone_with_too_many_children() {
        let expr = OverlayExpression::new(
            create_string_expr("abcdef"),
            create_string_expr("xyz"),
            create_int_expr(2),
            None,
            Column::new("result", TypeId::VarChar),
        );

        let new_children = vec![
            create_string_expr("123456"),
            create_string_expr("789"),
            create_int_expr(3),
            create_int_expr(2),
            create_int_expr(1),
        ];

        expr.clone_with_children(new_children);
    }
}
