use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::common::rid::RID;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregationType {
    Count,
    CountStar,
    Sum,
    Min,
    Max,
    Avg,
    StdDev,
    Variance,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpression {
    agg_type: AggregationType,
    children: Vec<Arc<Expression>>,
    return_type: Column,
    alias: Option<String>,
    function_name: String,
}

impl AggregateExpression {
    pub fn new(
        agg_type: AggregationType,
        children: Vec<Arc<Expression>>,
        return_type: Column,
        function_name: String,
    ) -> Self {
        Self {
            agg_type,
            children,
            return_type,
            alias: None,
            function_name,
        }
    }

    pub fn with_alias(mut self, alias: String) -> Self {
        self.alias = Some(alias);
        self
    }

    pub fn get_column_name(&self) -> String {
        if let Some(alias) = &self.alias {
            return alias.clone();
        }

        // If no alias, generate name based on convention
        match self.agg_type {
            AggregationType::CountStar => "COUNT_star".to_string(),
            _ if self.children.is_empty() => self.function_name.clone(),
            _ => {
                let arg = self.get_arg();
                let expr_str = match arg.as_ref() {
                    Expression::ColumnRef(col_ref) => {
                        // Extract just the column name without any table prefix
                        let full_name = col_ref.get_return_type().get_name();
                        // If the column name contains a dot (table.column), take just the column part
                        if let Some(idx) = full_name.rfind('.') {
                            full_name[idx + 1..].to_string()
                        } else {
                            full_name.to_string()
                        }
                    }
                    Expression::Constant(const_expr) => const_expr.to_string(),
                    Expression::Arithmetic(arith_expr) => arith_expr.to_string(),
                    Expression::Function(func_expr) => func_expr.to_string(),
                    _ => "expr".to_string(),
                };
                format!("{}_{}", self.function_name, expr_str.replace('.', "_"))
            }
        }
    }

    pub fn get_arg(&self) -> &Arc<Expression> {
        // For aggregate functions that take an argument (like SUM, AVG, etc.),
        // return the first child expression
        &self.children[0]
    }

    pub fn get_agg_type(&self) -> &AggregationType {
        &self.agg_type
    }

    pub fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    pub fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match &self.agg_type {
            AggregationType::Count | AggregationType::CountStar => {
                // For COUNT(*), return 1 for non-NULL tuples
                if self.children.is_empty() {
                    return Ok(Value::new(1_i64));
                }

                // For COUNT(expr), check if the expression evaluates to NULL
                let value = self.children[0].evaluate(tuple, schema)?;
                if value.is_null() {
                    Ok(Value::new(0_i64))
                } else {
                    Ok(Value::new(1_i64))
                }
            }
            AggregationType::Sum => {
                if self.children.is_empty() {
                    return Ok(Value::new(0_i64));
                }

                let value = self.children[0].evaluate(tuple, schema)?;
                if value.is_null() {
                    Ok(Value::new(0_i64))
                } else {
                    Ok(value)
                }
            }
            AggregationType::Min | AggregationType::Max => {
                if self.children.is_empty() {
                    return Ok(Value::new(Val::Null));
                }

                let value = self.children[0].evaluate(tuple, schema)?;
                Ok(value)
            }
            AggregationType::Avg => {
                if self.children.is_empty() {
                    return Ok(Value::new(Val::Null));
                }

                let value = self.children[0].evaluate(tuple, schema)?;
                if value.is_null() {
                    Ok(Value::new(Val::Null))
                } else {
                    Ok(value)
                }
            }
            AggregationType::StdDev | AggregationType::Variance => {
                if self.children.is_empty() {
                    return Ok(Value::new(Val::Null));
                }

                let value = self.children[0].evaluate(tuple, schema)?;
                if value.is_null() {
                    Ok(Value::new(Val::Null))
                } else {
                    Ok(value)
                }
            }
        }
    }
}

impl Display for AggregateExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}(", self.agg_type)?;
        for (i, child) in self.children.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", child)?;
        }
        write!(f, ")")
    }
}

impl ExpressionOps for AggregateExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        self.evaluate(tuple, schema)
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Create a merged schema and tuple with values from both tuples
        let merged_schema = Schema::merge(left_schema, right_schema);

        // Create values array for the merged tuple by combining values from both tuples
        let mut merged_values = Vec::new();
        merged_values.extend(left_tuple.get_values().iter().cloned());
        merged_values.extend(right_tuple.get_values().iter().cloned());

        let merged_tuple = Tuple::new(&merged_values, &merged_schema, RID::new(0, 0));

        // Evaluate the aggregate using the merged tuple and schema
        self.evaluate(&merged_tuple, &merged_schema)
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
        Arc::new(Expression::Aggregate(AggregateExpression::new(
            self.agg_type.clone(),
            children,
            self.return_type.clone(),
            self.function_name.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all child expressions first
        for child in &self.children {
            child.validate(schema)?;
        }

        // Then validate aggregate-specific requirements
        match self.agg_type {
            AggregationType::Count | AggregationType::CountStar => Ok(()), // Both COUNT and COUNT(*) can take any number of arguments
            _ if self.children.is_empty() => Err(ExpressionError::InvalidOperation(format!(
                "{:?} aggregate requires at least one argument",
                self.agg_type
            ))),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::types_db::type_id::TypeId;

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Decimal),
            Column::new("nullable", TypeId::Integer),
        ]);

        let tuple = Tuple::new(
            &[Value::new(1), Value::new(10.5), Value::new(Val::Null)],
            &schema,
            RID::new(0, 0),
        );

        (tuple, schema)
    }

    fn create_column_ref(col_idx: usize, col_name: &str, type_id: TypeId) -> Arc<Expression> {
        Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            col_idx,
            Column::new(col_name, type_id),
            vec![],
        )))
    }

    #[test]
    fn test_count_aggregate() {
        let (tuple, schema) = create_test_tuple();

        // Test COUNT(*)
        let count_star = AggregateExpression::new(
            AggregationType::CountStar,
            vec![],
            Column::new("count", TypeId::BigInt),
            "COUNT".to_string(),
        );

        assert_eq!(
            count_star.evaluate(&tuple, &schema).unwrap(),
            Value::new(1_i64)
        );

        // Test COUNT(column)
        let count_col = AggregateExpression::new(
            AggregationType::Count,
            vec![create_column_ref(0, "id", TypeId::Integer)],
            Column::new("count", TypeId::BigInt),
            "COUNT".to_string(),
        );

        assert_eq!(
            count_col.evaluate(&tuple, &schema).unwrap(),
            Value::new(1_i64)
        );

        // Test COUNT(nullable_column)
        let count_null = AggregateExpression::new(
            AggregationType::Count,
            vec![create_column_ref(2, "nullable", TypeId::Integer)],
            Column::new("count", TypeId::BigInt),
            "COUNT".to_string(),
        );

        assert_eq!(
            count_null.evaluate(&tuple, &schema).unwrap(),
            Value::new(0_i64)
        );
    }

    #[test]
    fn test_sum_aggregate() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::BigInt),
            Column::new("value", TypeId::Decimal),
            Column::new("nullable", TypeId::BigInt),
        ]);

        let tuple = Tuple::new(
            &[Value::new(1_i64), Value::new(10.5), Value::new(Val::Null)],
            &schema,
            RID::new(0, 0),
        );

        // Test SUM on decimal column
        let sum_decimal = AggregateExpression::new(
            AggregationType::Sum,
            vec![create_column_ref(1, "value", TypeId::Decimal)],
            Column::new("sum", TypeId::Decimal),
            "SUM".to_string(),
        );

        assert_eq!(
            sum_decimal.evaluate(&tuple, &schema).unwrap(),
            Value::new(10.5)
        );

        // Test SUM on integer column
        let sum_int = AggregateExpression::new(
            AggregationType::Sum,
            vec![create_column_ref(0, "id", TypeId::BigInt)],
            Column::new("sum", TypeId::BigInt),
            "SUM".to_string(),
        );

        assert_eq!(
            sum_int.evaluate(&tuple, &schema).unwrap(),
            Value::new(1_i64)
        );

        // Test SUM on nullable column
        let sum_null = AggregateExpression::new(
            AggregationType::Sum,
            vec![create_column_ref(2, "nullable", TypeId::BigInt)],
            Column::new("sum", TypeId::BigInt),
            "SUM".to_string(),
        );

        assert_eq!(
            sum_null.evaluate(&tuple, &schema).unwrap(),
            Value::new(0_i64)
        );
    }

    #[test]
    fn test_min_max_aggregate() {
        let (tuple, schema) = create_test_tuple();

        // Test MIN
        let min_expr = AggregateExpression::new(
            AggregationType::Min,
            vec![create_column_ref(1, "value", TypeId::Decimal)],
            Column::new("min", TypeId::Decimal),
            "MIN".to_string(),
        );

        assert_eq!(
            min_expr.evaluate(&tuple, &schema).unwrap(),
            Value::new(10.5)
        );

        // Test MAX
        let max_expr = AggregateExpression::new(
            AggregationType::Max,
            vec![create_column_ref(1, "value", TypeId::Decimal)],
            Column::new("max", TypeId::Decimal),
            "MAX".to_string(),
        );

        assert_eq!(
            max_expr.evaluate(&tuple, &schema).unwrap(),
            Value::new(10.5)
        );

        // Test MIN on nullable column
        let min_null = AggregateExpression::new(
            AggregationType::Min,
            vec![create_column_ref(2, "nullable", TypeId::Integer)],
            Column::new("min", TypeId::Integer),
            "MIN".to_string(),
        );

        assert_eq!(
            min_null.evaluate(&tuple, &schema).unwrap(),
            Value::new(Val::Null)
        );
    }

    #[test]
    fn test_avg_aggregate() {
        let (tuple, schema) = create_test_tuple();

        // Test AVG on decimal column
        let avg_decimal = AggregateExpression::new(
            AggregationType::Avg,
            vec![create_column_ref(1, "value", TypeId::Decimal)],
            Column::new("avg", TypeId::Decimal),
            "AVG".to_string(),
        );

        assert_eq!(
            avg_decimal.evaluate(&tuple, &schema).unwrap(),
            Value::new(10.5)
        );

        // Test AVG on nullable column
        let avg_null = AggregateExpression::new(
            AggregationType::Avg,
            vec![create_column_ref(2, "nullable", TypeId::Integer)],
            Column::new("avg", TypeId::Integer),
            "AVG".to_string(),
        );

        assert_eq!(
            avg_null.evaluate(&tuple, &schema).unwrap(),
            Value::new(Val::Null)
        );
    }

    #[test]
    fn test_column_name_generation() {
        // Test COUNT(*) column name
        let count_star = AggregateExpression::new(
            AggregationType::CountStar,
            vec![],
            Column::new("count", TypeId::BigInt),
            "COUNT".to_string(),
        );
        assert_eq!(count_star.get_column_name(), "COUNT_star");

        // Test regular aggregate column name
        let sum_expr = AggregateExpression::new(
            AggregationType::Sum,
            vec![create_column_ref(0, "value", TypeId::Decimal)],
            Column::new("sum", TypeId::Decimal),
            "SUM".to_string(),
        );
        assert_eq!(sum_expr.get_column_name(), "SUM_value");

        // Test with alias
        let aliased_sum = sum_expr.with_alias("total".to_string());
        assert_eq!(aliased_sum.get_column_name(), "total");
    }

    #[test]
    fn test_validate() {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Decimal),
        ]);

        // Valid COUNT(*)
        let count_star = AggregateExpression::new(
            AggregationType::CountStar,
            vec![],
            Column::new("count", TypeId::BigInt),
            "COUNT".to_string(),
        );
        assert!(count_star.validate(&schema).is_ok());

        // Invalid SUM (no arguments)
        let invalid_sum = AggregateExpression::new(
            AggregationType::Sum,
            vec![],
            Column::new("sum", TypeId::Decimal),
            "SUM".to_string(),
        );
        assert!(invalid_sum.validate(&schema).is_err());
    }
}
