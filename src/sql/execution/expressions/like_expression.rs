use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub struct LikeExpression {
    children: Vec<Arc<Expression>>, // Store expr and pattern as children
    escape_char: Option<char>,
    negated: bool,
    case_sensitive: bool,
    return_type: Column,
}

impl LikeExpression {
    pub fn new(
        expr: Arc<Expression>,
        pattern: Arc<Expression>,
        escape_char: Option<char>,
        negated: bool,
        case_sensitive: bool,
        return_type: Column,
    ) -> Self {
        Self {
            children: vec![expr, pattern],
            escape_char,
            negated,
            case_sensitive,
            return_type,
        }
    }

    fn get_expr(&self) -> &Arc<Expression> {
        &self.children[0]
    }

    fn get_pattern(&self) -> &Arc<Expression> {
        &self.children[1]
    }

    fn matches_pattern(&self, value: &str, pattern: &str, escape: Option<char>) -> bool {
        let mut pattern_chars = pattern.chars().peekable();
        let mut value_chars = value.chars().peekable();
        let mut escaping = false;

        while pattern_chars.peek().is_some() || value_chars.peek().is_some() {
            let pattern_char = pattern_chars.peek();

            // Handle escape character
            if let Some(esc) = escape {
                if !escaping && pattern_char == Some(&esc) {
                    pattern_chars.next(); // Consume escape character
                    escaping = true;
                    continue;
                }
            }

            match pattern_char {
                None => return value_chars.peek().is_none(), // Pattern exhausted
                Some(&'%') if !escaping => {
                    pattern_chars.next(); // Consume %

                    // Try to match the rest of the pattern at each position
                    let remaining_pattern: String = pattern_chars.clone().collect();
                    if remaining_pattern.is_empty() {
                        return true; // % at end matches anything
                    }

                    let remaining_value: String = value_chars.clone().collect();
                    for i in 0..=remaining_value.len() {
                        let test_value = &remaining_value[i..];
                        if self.matches_pattern(test_value, &remaining_pattern, escape) {
                            return true;
                        }
                    }
                    return false;
                }
                Some(&'_') if !escaping => {
                    pattern_chars.next(); // Consume _
                    match value_chars.next() {
                        Some(_) => (),        // Match any single character
                        None => return false, // No character to match
                    }
                }
                Some(&pattern_ch) => {
                    pattern_chars.next(); // Consume pattern character
                    match value_chars.next() {
                        Some(value_ch) => {
                            // If we're escaping or if the pattern character is not a wildcard,
                            // do a literal match
                            let matches = if self.case_sensitive {
                                value_ch == pattern_ch
                            } else {
                                value_ch.to_ascii_lowercase() == pattern_ch.to_ascii_lowercase()
                            };
                            if !matches {
                                return false;
                            }
                        }
                        None => return false,
                    }
                }
            }
            escaping = false;
        }

        value_chars.peek().is_none() // True if both strings are exhausted
    }
}

impl ExpressionOps for LikeExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.get_expr().evaluate(tuple, schema)?;
        let pattern = self.get_pattern().evaluate(tuple, schema)?;

        let value_str = value.to_string();
        let pattern_str = pattern.to_string();

        let matches = self.matches_pattern(&value_str, &pattern_str, self.escape_char);
        Ok(Value::new(if self.negated { !matches } else { matches }))
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        let value =
            self.get_expr()
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
        let pattern =
            self.get_pattern()
                .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        let value_str = value.to_string();
        let pattern_str = pattern.to_string();

        let matches = self.matches_pattern(&value_str, &pattern_str, self.escape_char);
        Ok(Value::new(if self.negated { !matches } else { matches }))
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
        assert_eq!(
            children.len(),
            2,
            "LikeExpression requires exactly two children"
        );
        Arc::new(Expression::Like(LikeExpression::new(
            children[0].clone(),
            children[1].clone(),
            self.escape_char,
            self.negated,
            self.case_sensitive,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate children
        self.get_expr().validate(schema)?;
        self.get_pattern().validate(schema)?;

        // Ensure the expression returns a string type
        let expr_type = self.get_expr().get_return_type().get_type();
        let pattern_type = self.get_pattern().get_return_type().get_type();

        use crate::types_db::type_id::TypeId;
        if !matches!(expr_type, TypeId::VarChar | TypeId::Char) {
            return Err(ExpressionError::InvalidOperation(format!(
                "LIKE expression requires string input, got {:?}",
                expr_type
            )));
        }

        if !matches!(pattern_type, TypeId::VarChar | TypeId::Char) {
            return Err(ExpressionError::InvalidOperation(format!(
                "LIKE pattern must be a string, got {:?}",
                pattern_type
            )));
        }

        Ok(())
    }
}

impl Display for LikeExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {}{}LIKE {}",
            self.get_expr(),
            if !self.case_sensitive { "I" } else { "" },
            if self.negated { "NOT " } else { "" },
            self.get_pattern()
        )?;
        if let Some(esc) = self.escape_char {
            write!(f, " ESCAPE '{}'", esc)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Val;

    fn create_test_expression(
        value: &str,
        pattern: &str,
        escape_char: Option<char>,
        negated: bool,
        case_sensitive: bool,
    ) -> LikeExpression {
        let value_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(value),
            Column::new("test_val", TypeId::VarChar),
            vec![],
        )));
        let pattern_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(pattern),
            Column::new("test_pattern", TypeId::VarChar),
            vec![],
        )));
        LikeExpression::new(
            value_expr,
            pattern_expr,
            escape_char,
            negated,
            case_sensitive,
            Column::new("result", TypeId::Boolean),
        )
    }

    #[test]
    fn test_basic_like() {
        let expr = create_test_expression("hello", "hello", None, false, true);
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = expr.evaluate(&tuple, &schema).unwrap();
        match result.get_val() {
            Val::Boolean(b) => assert!(*b),
            _ => panic!("Expected boolean result"),
        }
    }

    #[test]
    fn test_wildcard_patterns() {
        let test_cases = vec![
            ("hello", "h%", true),
            ("hello", "%o", true),
            ("hello", "h%o", true),
            ("hello", "h_llo", true),
            ("hello", "_ello", true),
            ("hello", "hel%", true),
            ("hello", "h%l%o", true),
            ("hello", "h_ll_", true),
            ("hello", "world", false),
            ("hello", "h%x", false),
            ("hello", "h_x", false),
        ];

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        for (value, pattern, expected) in test_cases {
            let expr = create_test_expression(value, pattern, None, false, true);
            let result = expr.evaluate(&tuple, &schema).unwrap();
            match result.get_val() {
                Val::Boolean(b) => assert_eq!(
                    *b, expected,
                    "Failed for value '{}' with pattern '{}'",
                    value, pattern
                ),
                _ => panic!("Expected boolean result"),
            }
        }
    }

    #[test]
    fn test_escape_character() {
        let test_cases = vec![
            // Test escaping special characters in pattern
            ("hello%world", "hello\\%world", true), // Escaped % in pattern matches literal % in value
            ("hello_world", "hello\\_world", true), // Escaped _ in pattern matches literal _ in value
            ("hello%", "hello\\%", true),           // Escaped % at end
            ("hello_", "hello\\_", true),           // Escaped _ at end
            ("hello\\world", "hello\\\\world", true), // Escaped backslash
            // Test unescaped wildcards in pattern
            ("hello%world", "hello%world", true), // % in pattern matches % in value
            ("helloXworld", "hello%world", true), // % in pattern matches any character
            ("hello_world", "hello_world", true), // _ in pattern matches _ in value
            ("helloXworld", "hello_world", true), // _ in pattern matches any character
            // Test literal matches
            ("hello\\%world", "hello\\\\%world", true), // Escaped backslash followed by %
            ("hello\\_world", "hello\\\\_world", true), // Escaped backslash followed by _
        ];

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        for (value, pattern, expected) in test_cases {
            let expr = create_test_expression(value, pattern, Some('\\'), false, true);
            let result = expr.evaluate(&tuple, &schema).unwrap();
            match result.get_val() {
                Val::Boolean(b) => assert_eq!(
                    *b, expected,
                    "Failed for value '{}' with pattern '{}'",
                    value, pattern
                ),
                _ => panic!("Expected boolean result"),
            }
        }
    }

    #[test]
    fn test_case_sensitivity() {
        let test_cases = vec![
            ("HELLO", "hello", false, false), // Case-sensitive, should not match
            ("HELLO", "hello", true, true),   // Case-insensitive, should match
            ("Hello", "HELLO", false, false), // Case-sensitive, should not match
            ("Hello", "HELLO", true, true),   // Case-insensitive, should match
            ("hello", "HELLO", false, false), // Case-sensitive, should not match
            ("hello", "HELLO", true, true),   // Case-insensitive, should match
        ];

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        for (value, pattern, case_insensitive, expected) in test_cases {
            let expr = create_test_expression(value, pattern, None, false, !case_insensitive);
            let result = expr.evaluate(&tuple, &schema).unwrap();
            match result.get_val() {
                Val::Boolean(b) => assert_eq!(
                    *b, expected,
                    "Failed for value '{}' with pattern '{}' (case_insensitive: {})",
                    value, pattern, case_insensitive
                ),
                _ => panic!("Expected boolean result"),
            }
        }
    }

    #[test]
    fn test_negation() {
        let test_cases = vec![
            ("hello", "hello", true, false), // Negated, exact match
            ("hello", "world", true, true),  // Negated, no match
            ("hello", "h%", true, false),    // Negated, wildcard match
            ("hello", "x%", true, true),     // Negated, wildcard no match
        ];

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        for (value, pattern, negated, expected) in test_cases {
            let expr = create_test_expression(value, pattern, None, negated, true);
            let result = expr.evaluate(&tuple, &schema).unwrap();
            match result.get_val() {
                Val::Boolean(b) => assert_eq!(
                    *b, expected,
                    "Failed for value '{}' with pattern '{}' (negated: {})",
                    value, pattern, negated
                ),
                _ => panic!("Expected boolean result"),
            }
        }
    }

    #[test]
    fn test_empty_strings() {
        let test_cases = vec![
            ("", "", true),       // Empty string matches empty pattern
            ("", "%", true),      // Empty string matches wildcard
            ("", "_", false),     // Empty string doesn't match single character
            ("hello", "", false), // Non-empty string doesn't match empty pattern
        ];

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        for (value, pattern, expected) in test_cases {
            let expr = create_test_expression(value, pattern, None, false, true);
            let result = expr.evaluate(&tuple, &schema).unwrap();
            match result.get_val() {
                Val::Boolean(b) => assert_eq!(
                    *b, expected,
                    "Failed for value '{}' with pattern '{}'",
                    value, pattern
                ),
                _ => panic!("Expected boolean result"),
            }
        }
    }
}
