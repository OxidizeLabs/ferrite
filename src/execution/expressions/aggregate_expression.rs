use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::execution::expressions::abstract_expression::{Expression, ExpressionOps};
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
        // Determine return type based on aggregation type
        let ret_type = match agg_type {
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

        Self {
            agg_type,
            arg,
            ret_type,
            children,
        }
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
