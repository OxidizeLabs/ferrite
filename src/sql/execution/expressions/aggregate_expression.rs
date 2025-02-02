use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub enum AggregationType {
    CountStar, // COUNT(*)
    Count,     // COUNT(expr)
    Sum,       // SUM(expr)
    Min,       // MIN(expr)
    Max,       // MAX(expr)
    Avg,       // AVG(expr)
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggregateExpression {
    agg_type: AggregationType,
    arg: Arc<Expression>,
    ret_type: Column,
    children: Vec<Arc<Expression>>,
}

impl AggregateExpression {
    pub fn new(
        agg_type: AggregationType,
        arg: Arc<Expression>,
        children: Vec<Arc<Expression>>,
    ) -> Self {
        // Format the column name based on the aggregate function and its argument
        let col_name = match &agg_type {
            AggregationType::CountStar => "COUNT(*)".to_string(),
            _ => format!(
                "{}({})",
                agg_type.to_string(),
                arg.get_return_type().get_name()
            ),
        };

        // Determine return type based on aggregation type
        let mut ret_type = match agg_type {
            AggregationType::CountStar | AggregationType::Count => {
                Column::new("count_result", TypeId::BigInt)
            }
            AggregationType::Sum => {
                // Sum returns same type as input for integers, Decimal for floating point
                match arg.get_return_type().get_type() {
                    TypeId::Integer | TypeId::BigInt => arg.get_return_type().clone(),
                    _ => Column::new("sum_result", TypeId::Decimal),
                }
            }
            AggregationType::Min | AggregationType::Max => {
                // Min/Max return same type as input
                arg.get_return_type().clone()
            }
            AggregationType::Avg => {
                // Average always returns Decimal
                Column::new("avg_result", TypeId::Decimal)
            }
        };

        // Set the formatted name for the return type
        ret_type = ret_type.with_name(&col_name);

        Self {
            agg_type,
            arg,
            ret_type,
            children,
        }
    }

    pub fn with_return_type(mut self, ret_type: Column) -> Self {
        self.ret_type = ret_type;
        self
    }

    pub fn get_agg_type(&self) -> &AggregationType {
        &self.agg_type
    }

    pub fn get_arg(&self) -> &Arc<Expression> {
        &self.arg
    }
}

impl ExpressionOps for AggregateExpression {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        // Individual tuple evaluation doesn't make sense for aggregates
        // This will be handled by the AggregationExecutor
        Err(ExpressionError::InvalidOperation(
            "Cannot evaluate aggregate function on individual tuple".to_string(),
        ))
    }

    fn evaluate_join(
        &self,
        _left_tuple: &Tuple,
        _left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Same as evaluate
        Err(ExpressionError::InvalidOperation(
            "Cannot evaluate aggregate function on individual tuple".to_string(),
        ))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        &self.children[child_idx]
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.ret_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 1 {
            panic!("AggregateExpression requires exactly one child");
        }

        Arc::new(Expression::Aggregate(Self::new(
            self.agg_type.clone(),
            children[0].clone(),
            children,
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // For COUNT(*), no need to validate the argument
        if matches!(self.agg_type, AggregationType::CountStar) {
            return Ok(());
        }

        // Validate the argument expression
        self.arg.validate(schema)?;

        // Validate all child expressions
        for child in &self.children {
            child.validate(schema)?;
        }

        Ok(())
    }
}

impl Display for AggregationType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AggregationType::CountStar => write!(f, "COUNT(*)"),
            AggregationType::Count => write!(f, "COUNT"),
            AggregationType::Sum => write!(f, "SUM"),
            AggregationType::Min => write!(f, "MIN"),
            AggregationType::Max => write!(f, "MAX"),
            AggregationType::Avg => write!(f, "AVG"),
        }
    }
}

impl Display for AggregateExpression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if f.alternate() {
            match self.agg_type {
                // Special case for COUNT(*) since it doesn't have an argument
                AggregationType::CountStar => write!(f, "COUNT(*)"),
                // All other aggregate functions format as: function_name(argument)
                _ => write!(f, "{}({:#})", self.agg_type, self.arg),
            }
        } else {
            match self.agg_type {
                // Special case for COUNT(*) since it doesn't have an argument
                AggregationType::CountStar => write!(f, "COUNT(*)"),
                // All other aggregate functions format as: function_name(argument)
                _ => write!(f, "{}({})", self.agg_type, self.arg),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    #[test]
    fn test_aggregate_expression_display() {
        // Test COUNT(*)
        let dummy_arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("dummy", TypeId::Integer),
            vec![],
        )));
        let count_star = Expression::Aggregate(AggregateExpression::new(
            AggregationType::CountStar,
            dummy_arg,
            vec![],
        ));

        assert_eq!(count_star.to_string(), "COUNT(*)");
        assert_eq!(format!("{:#}", count_star), "COUNT(*)");

        // Test COUNT(column)
        let col_arg = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            0,
            Column::new("id", TypeId::Integer),
            vec![],
        )));
        let count_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Count,
            col_arg.clone(),
            vec![],
        ));

        assert_eq!(count_expr.to_string(), "COUNT(id)");
        assert_eq!(format!("{:#}", count_expr), "COUNT(Col#0.0)");

        // Test SUM(column)
        let sum_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            col_arg.clone(),
            vec![],
        ));

        assert_eq!(sum_expr.to_string(), "SUM(id)");
        assert_eq!(format!("{:#}", sum_expr), "SUM(Col#0.0)");

        // Test AVG(column)
        let avg_expr = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Avg,
            col_arg.clone(),
            vec![],
        ));

        assert_eq!(avg_expr.to_string(), "AVG(id)");
        assert_eq!(format!("{:#}", avg_expr), "AVG(Col#0.0)");

        // Test with constant argument
        let const_arg = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let sum_const = Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            const_arg,
            vec![],
        ));

        assert_eq!(sum_const.to_string(), "SUM(42)");
        assert_eq!(format!("{:#}", sum_const), "SUM(Constant(42))");
    }
}
