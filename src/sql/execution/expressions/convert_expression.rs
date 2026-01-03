use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct ConvertExpression {
    expr: Arc<Expression>,
    target_type: Option<TypeId>,
    charset: Option<String>,
    is_try: bool,
    styles: Vec<Arc<Expression>>,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl ConvertExpression {
    pub fn new(
        expr: Arc<Expression>,
        target_type: Option<TypeId>,
        charset: Option<String>,
        is_try: bool,
        styles: Vec<Arc<Expression>>,
        return_type: Column,
    ) -> Self {
        let mut children = vec![expr.clone()];
        children.extend(styles.iter().cloned());

        Self {
            expr,
            target_type,
            charset,
            is_try,
            styles,
            return_type,
            children,
        }
    }
}

impl ExpressionOps for ConvertExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        let value = self.expr.evaluate(tuple, schema)?;

        // Handle character set conversion if specified
        if let Some(charset) = &self.charset {
            // For now, we only support UTF8 charset conversion
            if charset.to_lowercase() == "utf8" || charset.to_lowercase() == "utf8mb4" {
                return Ok(value); // Already UTF8
            }
            return Err(ExpressionError::InvalidOperation(format!(
                "Unsupported character set: {}",
                charset
            )));
        }

        // Handle type conversion
        if let Some(target_type) = self.target_type {
            match value.cast_to(target_type) {
                Ok(converted) => Ok(converted),
                Err(e) => {
                    if self.is_try {
                        Ok(Value::new_with_type(Val::Null, target_type))
                    } else {
                        Err(ExpressionError::InvalidOperation(e))
                    }
                },
            }
        } else {
            Ok(value)
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // For join evaluation, evaluate the inner expression using join context
        let value = self
            .expr
            .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;

        // Then apply the conversion logic as in regular evaluate
        if let Some(charset) = &self.charset {
            if charset.to_lowercase() == "utf8" || charset.to_lowercase() == "utf8mb4" {
                return Ok(value);
            }
            return Err(ExpressionError::InvalidOperation(format!(
                "Unsupported character set: {}",
                charset
            )));
        }

        if let Some(target_type) = self.target_type {
            match value.cast_to(target_type) {
                Ok(converted) => Ok(converted),
                Err(e) => {
                    if self.is_try {
                        Ok(Value::new_with_type(Val::Null, target_type))
                    } else {
                        Err(ExpressionError::InvalidOperation(e))
                    }
                },
            }
        } else {
            Ok(value)
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
        if children.is_empty() {
            return Arc::new(Expression::Convert(self.clone()));
        }

        let expr = children[0].clone();
        let styles = children.into_iter().skip(1).collect();

        Arc::new(Expression::Convert(ConvertExpression::new(
            expr,
            self.target_type,
            self.charset.clone(),
            self.is_try,
            styles,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the inner expression
        self.expr.validate(schema)?;

        // Validate all style expressions if any
        for style in &self.styles {
            style.validate(schema)?;
        }

        Ok(())
    }
}

impl Display for ConvertExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "CONVERT(")?;
        if self.is_try {
            write!(f, "TRY_")?;
        }
        write!(f, "{}", self.expr)?;
        if let Some(typ) = self.target_type {
            write!(f, " AS {:?}", typ)?;
        }
        if let Some(charset) = &self.charset {
            write!(f, " USING {}", charset)?;
        }
        write!(f, ")")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("int_col", TypeId::Integer),
            Column::new("str_col", TypeId::VarChar),
            Column::new("dec_col", TypeId::Decimal),
        ])
    }

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = create_test_schema();
        let values = vec![
            Value::new(42),                   // int_col
            Value::new("test string"),        // str_col
            Value::new(std::f64::consts::PI), // dec_col
        ];
        let tuple = Tuple::new(&values, &schema, RID::new(0, 0));
        (tuple, schema)
    }

    #[test]
    fn test_basic_type_conversion() {
        let (tuple, schema) = create_test_tuple();

        // Create a convert expression to convert integer to decimal
        let int_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("int_col", TypeId::Integer),
            vec![],
        )));

        let convert_expr = ConvertExpression::new(
            int_col,
            Some(TypeId::Decimal),
            None,
            false,
            vec![],
            Column::new("result", TypeId::Decimal),
        );

        let result = convert_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_type_id(), TypeId::Decimal);
        assert_eq!(result.to_string(), "42");
    }

    #[test]
    fn test_try_convert() {
        let (tuple, schema) = create_test_tuple();

        // Try to convert string to integer (should fail gracefully)
        let str_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("str_col", TypeId::VarChar),
            vec![],
        )));

        let convert_expr = ConvertExpression::new(
            str_col,
            Some(TypeId::Integer),
            None,
            true, // TRY_CONVERT
            vec![],
            Column::new("result", TypeId::Integer),
        );

        let result = convert_expr.evaluate(&tuple, &schema).unwrap();
        assert!(result.is_null()); // Should return NULL instead of error
    }

    #[test]
    fn test_charset_conversion() {
        let (tuple, schema) = create_test_tuple();

        // Test UTF8 charset conversion (should pass through)
        let str_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("str_col", TypeId::VarChar),
            vec![],
        )));

        let convert_expr = ConvertExpression::new(
            str_col.clone(),
            None,
            Some("utf8".to_string()),
            false,
            vec![],
            Column::new("result", TypeId::VarChar),
        );

        let result = convert_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.to_string(), "test string");

        // Test unsupported charset (should error)
        let convert_expr = ConvertExpression::new(
            str_col,
            None,
            Some("latin1".to_string()),
            false,
            vec![],
            Column::new("result", TypeId::VarChar),
        );

        assert!(convert_expr.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_convert_with_join() {
        let schema1 = Schema::new(vec![Column::new("left_int", TypeId::Integer)]);
        let schema2 = Schema::new(vec![Column::new("right_str", TypeId::VarChar)]);

        let left_tuple = Tuple::new(&[Value::new(123)], &schema1, RID::new(0, 0));
        let right_tuple = Tuple::new(&[Value::new("right value")], &schema2, RID::new(0, 0));

        let left_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("left_int", TypeId::Integer),
            vec![],
        )));

        let convert_expr = ConvertExpression::new(
            left_col,
            Some(TypeId::VarChar),
            None,
            false,
            vec![],
            Column::new("result", TypeId::VarChar),
        );

        let result = convert_expr
            .evaluate_join(&left_tuple, &schema1, &right_tuple, &schema2)
            .unwrap();

        assert_eq!(result.get_type_id(), TypeId::VarChar);
        assert_eq!(result.to_string(), "123");
    }

    #[test]
    fn test_display() {
        let expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("test_col", TypeId::Integer),
            vec![],
        )));

        // Test basic conversion
        let convert_expr = ConvertExpression::new(
            expr.clone(),
            Some(TypeId::Decimal),
            None,
            false,
            vec![],
            Column::new("result", TypeId::Decimal),
        );
        assert_eq!(convert_expr.to_string(), "CONVERT(test_col AS Decimal)");

        // Test with TRY_CONVERT
        let try_convert_expr = ConvertExpression::new(
            expr.clone(),
            Some(TypeId::Decimal),
            None,
            true,
            vec![],
            Column::new("result", TypeId::Decimal),
        );
        assert_eq!(
            try_convert_expr.to_string(),
            "CONVERT(TRY_test_col AS Decimal)"
        );

        // Test with charset
        let charset_expr = ConvertExpression::new(
            expr,
            None,
            Some("utf8".to_string()),
            false,
            vec![],
            Column::new("result", TypeId::VarChar),
        );
        assert_eq!(charset_expr.to_string(), "CONVERT(test_col USING utf8)");
    }

    #[test]
    fn test_validation() {
        let schema = create_test_schema();

        // Valid expression
        let valid_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("int_col", TypeId::Integer),
            vec![],
        )));

        let convert_expr = ConvertExpression::new(
            valid_col,
            Some(TypeId::Decimal),
            None,
            false,
            vec![],
            Column::new("result", TypeId::Decimal),
        );

        assert!(convert_expr.validate(&schema).is_ok());

        // Invalid column reference
        let invalid_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            10,
            Column::new("nonexistent", TypeId::Integer),
            vec![],
        )));

        let invalid_expr = ConvertExpression::new(
            invalid_col,
            Some(TypeId::Decimal),
            None,
            false,
            vec![],
            Column::new("result", TypeId::Decimal),
        );

        assert!(invalid_expr.validate(&schema).is_err());
    }

    #[test]
    fn test_clone_with_children() {
        let expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("test_col", TypeId::Integer),
            vec![],
        )));

        let original = ConvertExpression::new(
            expr.clone(),
            Some(TypeId::Decimal),
            None,
            false,
            vec![],
            Column::new("result", TypeId::Decimal),
        );

        // Test cloning with empty children
        let empty_clone = original.clone_with_children(vec![]);
        match empty_clone.as_ref() {
            Expression::Convert(conv) => {
                assert_eq!(conv.target_type, original.target_type);
                assert_eq!(conv.charset, original.charset);
                assert_eq!(conv.is_try, original.is_try);
            },
            _ => panic!("Expected Convert expression"),
        }

        // Test cloning with new children
        let new_expr = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            1,
            Column::new("other_col", TypeId::VarChar),
            vec![],
        )));
        let clone_with_new = original.clone_with_children(vec![new_expr]);
        match clone_with_new.as_ref() {
            Expression::Convert(conv) => {
                assert_eq!(conv.target_type, original.target_type);
                assert_eq!(conv.charset, original.charset);
                assert_eq!(conv.is_try, original.is_try);
                assert_eq!(conv.get_children().len(), 1);
            },
            _ => panic!("Expected Convert expression"),
        }
    }
}
