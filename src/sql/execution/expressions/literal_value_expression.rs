use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use sqlparser::ast::Value as SQLValue;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

/// Expression that represents a literal value from SQL
/// e.g., numbers, strings, booleans, NULL
#[derive(Debug, Clone, PartialEq)]
pub struct LiteralValueExpression {
    /// The literal value
    value: Value,
    /// Return type column
    return_type: Column,
    /// Child expressions (empty for literals)
    children: Vec<Arc<Expression>>,
}

impl LiteralValueExpression {
    pub fn new(sql_value: SQLValue) -> Result<Self, String> {
        let (value, type_id) = match sql_value {
            SQLValue::Number(n, _) => {
                if n.contains('.') {
                    // Parse as decimal if it contains a decimal point
                    (
                        Value::from(n.parse::<f64>().map_err(|e| e.to_string())?),
                        TypeId::Decimal,
                    )
                } else {
                    // Parse as integer otherwise
                    (
                        Value::from(n.parse::<i32>().map_err(|e| e.to_string())?),
                        TypeId::Integer,
                    )
                }
            }
            SQLValue::SingleQuotedString(s) | SQLValue::DoubleQuotedString(s) => {
                (Value::from(s.as_str()), TypeId::VarChar)
            }
            SQLValue::Boolean(b) => (Value::from(b), TypeId::Boolean),
            SQLValue::Null => (Value::new(Val::Null), TypeId::Invalid),
            _ => return Err(format!("Unsupported value type: {:?}", sql_value)),
        };

        let name = match &value.get_val() {
            Val::Boolean(b) => format!("{}", b),
            Val::Integer(i) => format!("{}", i),
            Val::Decimal(d) => format!("{}", d),
            Val::VarLen(s) => format!("'{}'", s),
            Val::Null => "NULL".to_string(),
            _ => "literal".to_string(),
        };

        Ok(Self {
            value,
            return_type: Column::new(&name, type_id),
            children: vec![],
        })
    }

    pub fn from_value(value: Value) -> Self {
        let type_id = value.get_type_id();
        let name = match &value.get_val() {
            Val::Boolean(b) => format!("{}", b),
            Val::Integer(i) => format!("{}", i),
            Val::Decimal(d) => format!("{}", d),
            Val::VarLen(s) => format!("'{}'", s),
            Val::Null => "NULL".to_string(),
            _ => "literal".to_string(),
        };

        Self {
            value,
            return_type: Column::new(&name, type_id),
            children: vec![],
        }
    }

    pub fn get_value(&self) -> &Value {
        &self.value
    }
}

impl ExpressionOps for LiteralValueExpression {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        // Literal values simply return their value
        Ok(self.value.clone())
    }

    fn evaluate_join(
        &self,
        _left_tuple: &Tuple,
        _left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Literal values simply return their value
        Ok(self.value.clone())
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        panic!("LiteralValueExpression has no children, tried to access index {}", child_idx);
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if !children.is_empty() {
            panic!("LiteralValueExpression cannot have children");
        }
        Arc::new(Expression::Literal(self.clone()))
    }

    fn validate(&self, _schema: &Schema) -> Result<(), ExpressionError> {
        // Literal values are always valid
        Ok(())
    }
}

impl Display for LiteralValueExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.value.get_val() {
            Val::Boolean(b) => write!(f, "{}", b),
            Val::TinyInt(i) => write!(f, "{}", i),
            Val::SmallInt(i) => write!(f, "{}", i),
            Val::Integer(i) => write!(f, "{}", i),
            Val::BigInt(i) => write!(f, "{}", i),
            Val::Decimal(d) => write!(f, "{}", d),
            Val::Timestamp(ts) => write!(f, "TIMESTAMP '{}'", ts),
            Val::VarLen(s) | Val::ConstLen(s) => write!(f, "'{}'", s),
            Val::Vector(v) => {
                write!(f, "[")?;
                for (i, val) in v.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", val)?;
                }
                write!(f, "]")
            }
            Val::Null => write!(f, "NULL"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use sqlparser::ast::Value as SQLValue;

    #[test]
    fn test_create_from_sql_value() {
        // Test numeric literals
        let int_expr = LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap();
        assert_eq!(int_expr.get_value(), &Value::new(42));
        assert_eq!(int_expr.get_return_type().get_type(), TypeId::Integer);

        let float_expr = LiteralValueExpression::new(SQLValue::Number("3.14".to_string(), false)).unwrap();
        assert_eq!(float_expr.get_value(), &Value::new(3.14));
        assert_eq!(float_expr.get_return_type().get_type(), TypeId::Decimal);

        // Test string literals
        let str_expr = LiteralValueExpression::new(SQLValue::SingleQuotedString("hello".to_string())).unwrap();
        assert_eq!(str_expr.get_value(), &Value::new("hello"));
        assert_eq!(str_expr.get_return_type().get_type(), TypeId::VarChar);

        // Test boolean literals
        let bool_expr = LiteralValueExpression::new(SQLValue::Boolean(true)).unwrap();
        assert_eq!(bool_expr.get_value(), &Value::new(true));
        assert_eq!(bool_expr.get_return_type().get_type(), TypeId::Boolean);

        // Test NULL literal
        let null_expr = LiteralValueExpression::new(SQLValue::Null).unwrap();
        assert_eq!(null_expr.get_value(), &Value::new(Val::Null));
        assert_eq!(null_expr.get_return_type().get_type(), TypeId::Invalid);
    }

    #[test]
    fn test_evaluate() {
        let schema = Schema::new(vec![Column::new("test", TypeId::Integer)]);
        let tuple = Tuple::new(&*vec![Value::new(1)], schema.clone(), RID::new(0, 0));

        // Create and evaluate a literal expression
        let expr = LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap();
        let result = expr.evaluate(&tuple, &schema).unwrap();
        
        assert_eq!(result, Value::new(42));
    }

    #[test]
    fn test_display() {
        // Test display formatting for different types
        let int_expr = LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap();
        assert_eq!(format!("{}", int_expr), "42");

        let float_expr = LiteralValueExpression::new(SQLValue::Number("3.14".to_string(), false)).unwrap();
        assert_eq!(format!("{}", float_expr), "3.14");

        let str_expr = LiteralValueExpression::new(SQLValue::SingleQuotedString("hello".to_string())).unwrap();
        assert_eq!(format!("{}", str_expr), "'hello'");

        let bool_expr = LiteralValueExpression::new(SQLValue::Boolean(true)).unwrap();
        assert_eq!(format!("{}", bool_expr), "true");

        let null_expr = LiteralValueExpression::new(SQLValue::Null).unwrap();
        assert_eq!(format!("{}", null_expr), "NULL");
    }

    #[test]
    fn test_from_value() {
        // Test creating from Value objects
        let int_value = Value::new(42);
        let expr = LiteralValueExpression::from_value(int_value.clone());
        
        assert_eq!(expr.get_value(), &int_value);
        assert_eq!(expr.get_return_type().get_type(), TypeId::Integer);
    }

    #[test]
    #[should_panic(expected = "LiteralValueExpression has no children")]
    fn test_get_child_at_panics() {
        let expr = LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap();
        expr.get_child_at(0); // Should panic
    }

    #[test]
    fn test_validate() {
        let schema = Schema::new(vec![Column::new("test", TypeId::Integer)]);
        let expr = LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap();
        
        // Validation should always succeed for literals
        assert!(expr.validate(&schema).is_ok());
    }
} 