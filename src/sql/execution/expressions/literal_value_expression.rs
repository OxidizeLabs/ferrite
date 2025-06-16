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
                    // Parse as float to ensure compatibility with Float columns
                    let parsed_value = n.parse::<f64>().map_err(|e| e.to_string())?;
                    let float_val = parsed_value as f32;
                    (Value::new_with_type(Val::Float(float_val), TypeId::Float), TypeId::Float)
                } else {
                    // Try parsing as integer types in order of size
                    // Start with smallest type that can hold the value
                    if let Ok(val) = n.parse::<i8>() {
                        (Value::from(val), TypeId::TinyInt)
                    } else if let Ok(val) = n.parse::<i16>() {
                        (Value::from(val), TypeId::SmallInt)
                    } else if let Ok(val) = n.parse::<i32>() {
                        (Value::from(val), TypeId::Integer)
                    } else if let Ok(val) = n.parse::<i64>() {
                        (Value::from(val), TypeId::BigInt)
                    } else {
                        return Err(format!("Number '{}' is too large to represent", n));
                    }
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
            Val::TinyInt(i) => format!("{}", i),
            Val::SmallInt(i) => format!("{}", i),
            Val::Integer(i) => format!("{}", i),
            Val::BigInt(i) => format!("{}", i),
            Val::Decimal(d) => format!("{}", d),
            Val::Float(f) => format!("{}", f),
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
            Val::TinyInt(i) => format!("{}", i),
            Val::SmallInt(i) => format!("{}", i),
            Val::Integer(i) => format!("{}", i),
            Val::BigInt(i) => format!("{}", i),
            Val::Decimal(d) => format!("{}", d),
            Val::Float(f) => format!("{}", f),
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
        panic!(
            "LiteralValueExpression has no children, tried to access index {}",
            child_idx
        );
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
            Val::Float(fl) => write!(f, "{}", fl),
            Val::Timestamp(ts) => write!(f, "TIMESTAMP {}", ts),
            Val::VarLen(s) | Val::ConstLen(s) => write!(f, "'{}'", s),
            Val::Vector(v) => write!(f, "{:?}", v),
            Val::Null => write!(f, "NULL"),
            Val::Struct => write!(f, "STRUCT"),
            Val::Date(d) => write!(f, "DATE {}", d),
            Val::Time(t) => write!(f, "TIME {}", t),
            Val::Interval(i) => write!(f, "INTERVAL {}", i),
            Val::Binary(b) => write!(f, "BIN {:?}", b),
            Val::JSON(j) => write!(f, "JSON {}", j),
            Val::UUID(u) => write!(f, "UUID {}", u),
            Val::Array(a) => write!(f, "[ {:?} ]", a),
            Val::Enum(a, b) => write!(f, "ENUM '{}: {}'", a, b),
            Val::Point(a, b) => write!(f, "POINT ({},{})", a, b),
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
        // Test numeric literals - 42 fits in TinyInt range
        let int_expr =
            LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap();
        assert_eq!(int_expr.get_value(), &Value::new(42i8));
        assert_eq!(int_expr.get_return_type().get_type(), TypeId::TinyInt);

        let float_expr =
            LiteralValueExpression::new(SQLValue::Number("3.14".to_string(), false)).unwrap();
        assert_eq!(float_expr.get_value(), &Value::new(3.14));
        assert_eq!(float_expr.get_return_type().get_type(), TypeId::Float);

        // Test string literals
        let str_expr =
            LiteralValueExpression::new(SQLValue::SingleQuotedString("hello".to_string())).unwrap();
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
        let tuple = Tuple::new(&*vec![Value::new(1)], &schema, RID::new(0, 0));

        // Create and evaluate a literal expression
        let expr = LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap();
        let result = expr.evaluate(&tuple, &schema).unwrap();

        assert_eq!(result, Value::new(42i8));
    }

    #[test]
    fn test_display() {
        // Test display formatting for different types
        let int_expr =
            LiteralValueExpression::new(SQLValue::Number("42".to_string(), false)).unwrap();
        assert_eq!(format!("{}", int_expr), "42");

        let float_expr =
            LiteralValueExpression::new(SQLValue::Number("3.14".to_string(), false)).unwrap();
        assert_eq!(format!("{}", float_expr), "3.14");

        let str_expr =
            LiteralValueExpression::new(SQLValue::SingleQuotedString("hello".to_string())).unwrap();
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

    #[test]
    fn test_numeric_type_selection() {
        // Test TinyInt range (i8: -128 to 127)
        let tiny_pos = LiteralValueExpression::new(SQLValue::Number("127".to_string(), false)).unwrap();
        assert_eq!(tiny_pos.get_return_type().get_type(), TypeId::TinyInt);
        assert_eq!(tiny_pos.get_value(), &Value::new(127i8));

        let tiny_neg = LiteralValueExpression::new(SQLValue::Number("-128".to_string(), false)).unwrap();
        assert_eq!(tiny_neg.get_return_type().get_type(), TypeId::TinyInt);
        assert_eq!(tiny_neg.get_value(), &Value::new(-128i8));

        // Test SmallInt range (i16: -32,768 to 32,767)
        let small_pos = LiteralValueExpression::new(SQLValue::Number("32767".to_string(), false)).unwrap();
        assert_eq!(small_pos.get_return_type().get_type(), TypeId::SmallInt);
        assert_eq!(small_pos.get_value(), &Value::new(32767i16));

        let small_neg = LiteralValueExpression::new(SQLValue::Number("-32768".to_string(), false)).unwrap();
        assert_eq!(small_neg.get_return_type().get_type(), TypeId::SmallInt);
        assert_eq!(small_neg.get_value(), &Value::new(-32768i16));

        // Test values that exceed TinyInt but fit in SmallInt
        let exceed_tiny = LiteralValueExpression::new(SQLValue::Number("128".to_string(), false)).unwrap();
        assert_eq!(exceed_tiny.get_return_type().get_type(), TypeId::SmallInt);
        assert_eq!(exceed_tiny.get_value(), &Value::new(128i16));

        // Test Integer range (i32: -2,147,483,648 to 2,147,483,647)
        let int_pos = LiteralValueExpression::new(SQLValue::Number("2147483647".to_string(), false)).unwrap();
        assert_eq!(int_pos.get_return_type().get_type(), TypeId::Integer);
        assert_eq!(int_pos.get_value(), &Value::new(2147483647i32));

        let int_neg = LiteralValueExpression::new(SQLValue::Number("-2147483648".to_string(), false)).unwrap();
        assert_eq!(int_neg.get_return_type().get_type(), TypeId::Integer);
        assert_eq!(int_neg.get_value(), &Value::new(-2147483648i32));

        // Test values that exceed SmallInt but fit in Integer
        let exceed_small = LiteralValueExpression::new(SQLValue::Number("32768".to_string(), false)).unwrap();
        assert_eq!(exceed_small.get_return_type().get_type(), TypeId::Integer);
        assert_eq!(exceed_small.get_value(), &Value::new(32768i32));

        // Test BigInt range (i64: -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807)
        let big_pos = LiteralValueExpression::new(SQLValue::Number("9223372036854775807".to_string(), false)).unwrap();
        assert_eq!(big_pos.get_return_type().get_type(), TypeId::BigInt);
        assert_eq!(big_pos.get_value(), &Value::new(9223372036854775807i64));

        let big_neg = LiteralValueExpression::new(SQLValue::Number("-9223372036854775808".to_string(), false)).unwrap();
        assert_eq!(big_neg.get_return_type().get_type(), TypeId::BigInt);
        assert_eq!(big_neg.get_value(), &Value::new(-9223372036854775808i64));

        // Test values that exceed Integer but fit in BigInt
        let exceed_int = LiteralValueExpression::new(SQLValue::Number("2147483648".to_string(), false)).unwrap();
        assert_eq!(exceed_int.get_return_type().get_type(), TypeId::BigInt);
        assert_eq!(exceed_int.get_value(), &Value::new(2147483648i64));

        // Test decimal values still parse as Decimal
        let decimal = LiteralValueExpression::new(SQLValue::Number("123.456".to_string(), false)).unwrap();
        assert_eq!(decimal.get_return_type().get_type(), TypeId::Decimal);
        assert_eq!(decimal.get_value(), &Value::new(123.456f64));

        // Test zero should be TinyInt (smallest type)
        let zero = LiteralValueExpression::new(SQLValue::Number("0".to_string(), false)).unwrap();
        assert_eq!(zero.get_return_type().get_type(), TypeId::TinyInt);
        assert_eq!(zero.get_value(), &Value::new(0i8));
    }

    #[test]
    fn test_numeric_overflow() {
        // Test value that exceeds i64::MAX should return an error
        let overflow_result = LiteralValueExpression::new(SQLValue::Number("18446744073709551616".to_string(), false));
        assert!(overflow_result.is_err());
        assert!(overflow_result.unwrap_err().contains("too large to represent"));

        // Test very large negative value
        let neg_overflow_result = LiteralValueExpression::new(SQLValue::Number("-18446744073709551616".to_string(), false));
        assert!(neg_overflow_result.is_err());
        assert!(neg_overflow_result.unwrap_err().contains("too large to represent"));
    }
}
