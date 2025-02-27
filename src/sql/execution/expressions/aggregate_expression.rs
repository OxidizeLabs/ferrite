use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
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
}

impl AggregateExpression {
    pub fn new(
        agg_type: AggregationType,
        children: Vec<Arc<Expression>>,
        return_type: Column,
    ) -> Self {
        // When creating a new AggregateExpression, ensure the return_type is correct based on the agg_type
        let actual_return_type = match agg_type {
            AggregationType::Count | AggregationType::CountStar => {
                Column::new("count", TypeId::BigInt)
            }
            AggregationType::Sum => {
                if children.is_empty() {
                    Column::new("sum", TypeId::BigInt)
                } else {
                    let child_type = children[0].get_return_type().get_type();
                    match child_type {
                        TypeId::Integer | TypeId::BigInt | TypeId::SmallInt | TypeId::TinyInt => {
                            Column::new("sum", TypeId::BigInt)
                        }
                        TypeId::Decimal => Column::new("sum", TypeId::Decimal),
                        _ => Column::new("sum", TypeId::BigInt),
                    }
                }
            }
            AggregationType::Min | AggregationType::Max => {
                if children.is_empty() {
                    Column::new(
                        if matches!(agg_type, AggregationType::Min) {
                            "min"
                        } else {
                            "max"
                        },
                        TypeId::BigInt,
                    )
                } else {
                    let child_type = children[0].get_return_type().get_type();
                    Column::new(
                        if matches!(agg_type, AggregationType::Min) {
                            "min"
                        } else {
                            "max"
                        },
                        child_type,
                    )
                }
            }
            AggregationType::Avg => Column::new("avg", TypeId::Decimal),
            AggregationType::StdDev => Column::new("stddev", TypeId::Decimal),
            AggregationType::Variance => Column::new("variance", TypeId::Decimal),
        };

        Self {
            agg_type,
            children,
            return_type: actual_return_type,
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
        // For aggregate expressions in a join context, we evaluate each child in the join context
        let mut child_values = Vec::new();
        for child in &self.children {
            child_values.push(child.evaluate_join(
                left_tuple,
                left_schema,
                right_tuple,
                right_schema,
            )?);
        }

        // Then evaluate the aggregate using the child values
        self.evaluate(left_tuple, left_schema)
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
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all child expressions first
        for child in &self.children {
            child.validate(schema)?;
        }

        // Then validate aggregate-specific requirements
        match self.agg_type {
            AggregationType::Count => Ok(()), // COUNT can take any number of arguments
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

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![
            Column::new("id", TypeId::Integer),
            Column::new("value", TypeId::Decimal),
        ]);

        let tuple = Tuple::new(
            &[Value::new(1), Value::new(10.5)],
            schema.clone(),
            RID::new(0, 0),
        );

        (tuple, schema)
    }

    #[test]
    fn test_count_aggregate() {
        let (tuple, schema) = create_test_tuple();

        // Test COUNT(*)
        let count_star = AggregateExpression::new(
            AggregationType::Count,
            vec![],
            Column::new("count", TypeId::BigInt),
        );

        assert_eq!(
            count_star.evaluate(&tuple, &schema).unwrap(),
            Value::new(1_i64)
        );

        // Test COUNT(column)
        let count_col = AggregateExpression::new(
            AggregationType::Count,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("id", TypeId::Integer),
                vec![],
            )))],
            Column::new("count", TypeId::BigInt),
        );

        assert_eq!(
            count_col.evaluate(&tuple, &schema).unwrap(),
            Value::new(1_i64)
        );
    }

    #[test]
    fn test_sum_aggregate() {
        let (tuple, schema) = create_test_tuple();

        let sum_expr = AggregateExpression::new(
            AggregationType::Sum,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                1,
                Column::new("value", TypeId::Decimal),
                vec![],
            )))],
            Column::new("sum", TypeId::Decimal),
        );

        assert_eq!(
            sum_expr.evaluate(&tuple, &schema).unwrap(),
            Value::new(10.5)
        );
    }
}
