use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use regex::Regex;
use std::cmp::PartialEq;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum RegexOperator {
    Match,        // ~ (matches)
    NotMatch,     // !~ (does not match)
    IMatch,       // ~* (matches case insensitive)
    NotIMatch,    // !~* (does not match case insensitive)
    SimilarTo,    // SIMILAR TO
    NotSimilarTo, // NOT SIMILAR TO
    RLike,        // RLIKE/REGEXP
    NotRLike,     // NOT RLIKE/NOT REGEXP
}

#[derive(Clone, Debug)]
pub struct RegexExpression {
    children: Vec<Arc<Expression>>, // Store expr and pattern as children
    operator: RegexOperator,
    escape_char: Option<String>,
    cached_regex: Option<Regex>,
    return_type: Column,
}

impl RegexExpression {
    pub fn new(
        expr: Arc<Expression>,
        pattern: Arc<Expression>,
        operator: RegexOperator,
        escape_char: Option<String>,
        return_type: Column,
    ) -> Self {
        Self {
            children: vec![expr, pattern],
            operator,
            escape_char,
            cached_regex: None,
            return_type,
        }
    }

    // Helper methods to access children
    fn expr(&self) -> &Arc<Expression> {
        &self.children[0]
    }

    fn pattern(&self) -> &Arc<Expression> {
        &self.children[1]
    }

    fn get_or_compile_regex(
        &mut self,
        pattern: &str,
        case_insensitive: bool,
    ) -> Result<&Regex, ExpressionError> {
        if self.cached_regex.is_none() {
            let mut pattern_str = pattern.to_string();

            // Handle SIMILAR TO pattern conversion
            if matches!(
                self.operator,
                RegexOperator::SimilarTo | RegexOperator::NotSimilarTo
            ) {
                pattern_str = self.convert_similar_to_pattern(&pattern_str)?;
            }

            // Add case insensitive flag if needed
            if case_insensitive
                || matches!(
                    self.operator,
                    RegexOperator::IMatch | RegexOperator::NotIMatch
                )
            {
                pattern_str = format!("(?i){}", pattern_str);
            }

            self.cached_regex = Some(Regex::new(&pattern_str).map_err(|e| {
                ExpressionError::InvalidOperation(format!("Invalid regex pattern: {}", e))
            })?);
        }
        Ok(self.cached_regex.as_ref().unwrap())
    }

    fn convert_similar_to_pattern(&self, pattern: &str) -> Result<String, ExpressionError> {
        let escape_char = self.escape_char.as_deref().unwrap_or("\\");
        let mut result = String::new();
        let mut chars = pattern.chars().peekable();

        while let Some(c) = chars.next() {
            if c == escape_char.chars().next().unwrap() {
                if let Some(&next) = chars.peek() {
                    // Handle escaped character
                    chars.next();
                    result.push_str(&regex::escape(&next.to_string()));
                }
            } else {
                match c {
                    '%' => result.push_str(".*"),
                    '_' => result.push('.'),
                    '[' => {
                        result.push('[');
                        while let Some(&next) = chars.peek() {
                            chars.next();
                            if next == ']' {
                                result.push(']');
                                break;
                            }
                            result.push_str(&regex::escape(&next.to_string()));
                        }
                    }
                    _ => result.push_str(&regex::escape(&c.to_string())),
                }
            }
        }

        Ok(format!("^{}$", result))
    }
}

impl ExpressionOps for RegexExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr().evaluate(tuple, schema)?;
        let pattern = self.pattern().evaluate(tuple, schema)?;

        let value_str = value.to_string();
        let pattern_str = pattern.to_string();

        // Create a mutable copy of self to update the cached regex
        let mut this = self.clone();

        // Determine if case insensitive based on operator
        let case_insensitive = matches!(
            self.operator,
            RegexOperator::IMatch
                | RegexOperator::NotIMatch
                | RegexOperator::RLike
                | RegexOperator::NotRLike
        );

        let regex = this.get_or_compile_regex(&pattern_str, case_insensitive)?;
        let matches = regex.is_match(&value_str);

        let result = match self.operator {
            RegexOperator::Match
            | RegexOperator::IMatch
            | RegexOperator::SimilarTo
            | RegexOperator::RLike => matches,
            RegexOperator::NotMatch
            | RegexOperator::NotIMatch
            | RegexOperator::NotSimilarTo
            | RegexOperator::NotRLike => !matches,
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
        match self
            .expr()
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
        {
            Ok(value) => {
                let pattern = self.pattern().evaluate_join(
                    left_tuple,
                    left_schema,
                    right_tuple,
                    right_schema,
                )?;
                let value_str = value.to_string();
                let pattern_str = pattern.to_string();

                let mut this = self.clone();
                let case_insensitive = matches!(
                    self.operator,
                    RegexOperator::IMatch
                        | RegexOperator::NotIMatch
                        | RegexOperator::RLike
                        | RegexOperator::NotRLike
                );

                let regex = this.get_or_compile_regex(&pattern_str, case_insensitive)?;
                let matches = regex.is_match(&value_str);

                let result = match self.operator {
                    RegexOperator::Match
                    | RegexOperator::IMatch
                    | RegexOperator::SimilarTo
                    | RegexOperator::RLike => matches,
                    RegexOperator::NotMatch
                    | RegexOperator::NotIMatch
                    | RegexOperator::NotSimilarTo
                    | RegexOperator::NotRLike => !matches,
                };

                Ok(Value::new(result))
            }
            Err(e) => Err(e),
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
        assert_eq!(
            children.len(),
            2,
            "RegexExpression requires exactly two children"
        );
        Arc::new(Expression::Regex(RegexExpression::new(
            children[0].clone(),
            children[1].clone(),
            self.operator.clone(),
            self.escape_char.clone(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate both expression and pattern
        self.expr().validate(schema)?;
        self.pattern().validate(schema)?;

        // Pattern should evaluate to a string type
        let pattern_type = self.pattern().get_return_type().get_type();
        if !matches!(pattern_type, TypeId::VarChar | TypeId::Char) {
            return Err(ExpressionError::InvalidOperation(format!(
                "Regex pattern must be a string type, got {:?}",
                pattern_type
            )));
        }

        Ok(())
    }
}

impl PartialEq for RegexExpression {
    fn eq(&self, other: &Self) -> bool {
        self.children == other.children
            && self.operator == other.operator
            && self.escape_char == other.escape_char
            && self.return_type == other.return_type
        // Skip comparing cached_regex since it's just a cache
    }
}

impl Display for RegexExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let op = match self.operator {
            RegexOperator::Match => "~",
            RegexOperator::NotMatch => "!~",
            RegexOperator::IMatch => "~*",
            RegexOperator::NotIMatch => "!~*",
            RegexOperator::SimilarTo => "SIMILAR TO",
            RegexOperator::NotSimilarTo => "NOT SIMILAR TO",
            RegexOperator::RLike => "RLIKE",
            RegexOperator::NotRLike => "NOT RLIKE",
        };
        write!(f, "{} {} {}", self.expr(), op, self.pattern())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;

    fn create_test_tuple(values: Vec<Value>, schema: Schema) -> Tuple {
        Tuple::new(&values, schema, RID::new(0, 0))
    }

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("text", TypeId::VarChar),
            Column::new("pattern", TypeId::VarChar),
        ])
    }

    #[test]
    fn test_basic_regex_match() {
        let schema = create_test_schema();

        let expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("hello world"),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let pattern = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("world$"),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let regex_expr = RegexExpression::new(
            expr,
            pattern,
            RegexOperator::Match,
            None,
            Column::new("result", TypeId::Boolean),
        );

        // Create tuple with dummy values matching schema column count
        let tuple = create_test_tuple(
            vec![Value::new("dummy_text"), Value::new("dummy_pattern")],
            schema.clone(),
        );
        let result = regex_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true));
    }

    #[test]
    fn test_case_sensitive_match() {
        let schema = create_test_schema();

        let test_cases = vec![
            ("Hello", "hello", RegexOperator::Match, false), // Case-sensitive no match
            ("Hello", "hello", RegexOperator::IMatch, true), // Case-insensitive match
            ("Hello", "HELLO", RegexOperator::Match, false), // Case-sensitive no match
            ("Hello", "HELLO", RegexOperator::IMatch, true), // Case-insensitive match
        ];

        for (text, pattern, operator, expected) in test_cases {
            let expr = Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(text),
                Column::new("const", TypeId::VarChar),
                vec![],
            )));

            let pattern = Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(pattern),
                Column::new("const", TypeId::VarChar),
                vec![],
            )));

            let regex_expr = RegexExpression::new(
                expr,
                pattern,
                operator,
                None,
                Column::new("result", TypeId::Boolean),
            );

            // Create tuple with dummy values matching schema column count
            let tuple = create_test_tuple(
                vec![Value::new("dummy_text"), Value::new("dummy_pattern")],
                schema.clone(),
            );
            let result = regex_expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(result, Value::new(expected));
        }
    }

    #[test]
    fn test_similar_to_patterns() {
        let schema = create_test_schema();

        let test_cases = vec![
            ("abc", "abc", true),     // Exact match
            ("abc", "a%c", true),     // Wildcard match
            ("abc", "a_c", true),     // Single character match
            ("abc", "a[b]c", true),   // Character class match
            ("abc", "a[cd]c", false), // Character class no match
            ("abc", "%b%", true),     // Multiple wildcards
            ("abc", "a%", true),      // Prefix match
            ("abc", "%c", true),      // Suffix match
            ("abc", "a\\%c", false),  // Escaped special character
            ("a%c", "a\\%c", true),   // Escaped special character match
        ];

        for (text, pattern, expected) in test_cases {
            let expr = Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(text),
                Column::new("const", TypeId::VarChar),
                vec![],
            )));

            let pattern = Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(pattern),
                Column::new("const", TypeId::VarChar),
                vec![],
            )));

            let regex_expr = RegexExpression::new(
                expr,
                pattern.clone(),
                RegexOperator::SimilarTo,
                Some("\\".to_string()),
                Column::new("result", TypeId::Boolean),
            );

            // Create tuple with dummy values matching schema column count
            let tuple = create_test_tuple(
                vec![Value::new("dummy_text"), Value::new("dummy_pattern")],
                schema.clone(),
            );
            let result = regex_expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(
                result,
                Value::new(expected),
                "Failed for text '{}' with pattern '{}'",
                text,
                pattern
            );
        }
    }

    #[test]
    fn test_rlike_patterns() {
        let schema = create_test_schema();

        let test_cases = vec![
            ("abc123", "^[a-z]+[0-9]+$", true), // Basic regex pattern
            ("ABC123", "^[a-z]+[0-9]+$", true), // Case insensitive
            ("abc", "^[0-9]+$", false),         // No match
            ("123abc", "\\d+[a-z]+", true),     // Regex special characters
            ("abc", "[aeiou]", true),           // Character class
            ("xyz", "[aeiou]", false),          // Character class no match
        ];

        for (text, pattern, expected) in test_cases {
            let expr = Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(text),
                Column::new("const", TypeId::VarChar),
                vec![],
            )));

            let pattern = Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(pattern),
                Column::new("const", TypeId::VarChar),
                vec![],
            )));

            let regex_expr = RegexExpression::new(
                expr,
                pattern.clone(),
                RegexOperator::RLike,
                None,
                Column::new("result", TypeId::Boolean),
            );

            // Create tuple with dummy values matching schema column count
            let tuple = create_test_tuple(
                vec![Value::new("dummy_text"), Value::new("dummy_pattern")],
                schema.clone(),
            );
            let result = regex_expr.evaluate(&tuple, &schema).unwrap();
            assert_eq!(
                result,
                Value::new(expected),
                "Failed for text '{}' with pattern '{}'",
                text,
                pattern
            );
        }
    }

    #[test]
    fn test_invalid_patterns() {
        let schema = create_test_schema();

        let expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("test"),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let pattern = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("[invalid"), // Invalid regex pattern
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let regex_expr = RegexExpression::new(
            expr,
            pattern,
            RegexOperator::Match,
            None,
            Column::new("result", TypeId::Boolean),
        );

        // Create tuple with dummy values matching schema column count
        let tuple = create_test_tuple(
            vec![Value::new("dummy_text"), Value::new("dummy_pattern")],
            schema.clone(),
        );
        assert!(regex_expr.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_validation() {
        let schema = create_test_schema();

        // Valid expression
        let expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("test"),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let pattern = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("test"),
            Column::new("const", TypeId::VarChar),
            vec![],
        )));

        let regex_expr = RegexExpression::new(
            expr.clone(),
            pattern.clone(),
            RegexOperator::Match,
            None,
            Column::new("result", TypeId::Boolean),
        );

        assert!(regex_expr.validate(&schema).is_ok());

        // Invalid pattern type
        let invalid_pattern = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));

        let invalid_regex_expr = RegexExpression::new(
            expr,
            invalid_pattern,
            RegexOperator::Match,
            None,
            Column::new("result", TypeId::Boolean),
        );

        assert!(invalid_regex_expr.validate(&schema).is_err());
    }
}
