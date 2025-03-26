use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::Value;
use log::trace;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum FilterType {
    Where,  // For WHERE clauses - filters rows before aggregation
    Having, // For HAVING clauses - filters groups after aggregation
}

#[derive(Clone, Debug, PartialEq)]
pub struct FilterExpression {
    filter_type: FilterType,
    aggregate: Option<Arc<Expression>>,  // Only present for HAVING clauses
    predicate: Arc<Expression>,
    return_type: Column,
    children: Vec<Arc<Expression>>,
}

impl FilterExpression {
    pub fn new_where(
        predicate: Arc<Expression>,
        return_type: Column,
    ) -> Self {
        let children = vec![predicate.clone()];
        Self {
            filter_type: FilterType::Where,
            aggregate: None,
            predicate,
            return_type,
            children,
        }
    }

    pub fn new_having(
        aggregate: Arc<Expression>,
        predicate: Arc<Expression>,
        return_type: Column,
    ) -> Self {
        let children = vec![aggregate.clone(), predicate.clone()];
        Self {
            filter_type: FilterType::Having,
            aggregate: Some(aggregate),
            predicate,
            return_type,
            children,
        }
    }

    pub fn get_filter_type(&self) -> FilterType {
        self.filter_type.clone()
    }

    pub fn get_predicate(&self) -> Arc<Expression> {
        self.predicate.clone()
    }

    pub fn get_aggregate(&self) -> &Option<Arc<Expression>> {
        &self.aggregate
    }

    fn contains_aggregate(expr: &Expression) -> bool {
        match expr {
            Expression::Aggregate(_) => true,
            _ => expr.get_children().iter().any(|child| Self::contains_aggregate(child)),
        }
    }
}

impl ExpressionOps for FilterExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match self.filter_type {
            FilterType::Where => {
                // For WHERE clauses, simply evaluate the predicate against the tuple
                let pred_result = self.predicate.evaluate(tuple, schema)?;
                if pred_result.as_bool().unwrap_or(false) {
                    Ok(Value::new(true))
                } else {
                    Ok(Value::new(false))
                }
            }
            FilterType::Having => {
                // For HAVING clauses, evaluate aggregate first against the original schema
                let agg_result = self.aggregate.as_ref()
                    .ok_or_else(|| ExpressionError::InvalidExpression("Missing aggregate for HAVING clause".to_string()))?
                    .evaluate(tuple, schema)?;

                // Create a new schema with just the aggregate column
                let agg_schema = Schema::new(vec![self.aggregate.as_ref().unwrap().get_return_type().clone()]);
                
                // Create a new tuple with just the aggregate result
                let agg_tuple = Tuple::new(&[agg_result], agg_schema.clone(), tuple.get_rid());
                
                // Now evaluate the predicate against the aggregate tuple and schema
                let pred_result = self.predicate.evaluate(&agg_tuple, &agg_schema)?;
                
                if pred_result.as_bool().unwrap_or(false) {
                    Ok(Value::new(true))
                } else {
                    Ok(Value::new(false))
                }
            }
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        match self.filter_type {
            FilterType::Where => {
                // For WHERE clauses, evaluate predicate on joined tuples
                let pred_result = self.predicate.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
                if pred_result.as_bool().unwrap_or(false) {
                    Ok(Value::new(true))
                } else {
                    Ok(Value::new(false))
                }
            }
            FilterType::Having => {
                // For HAVING clauses, evaluate aggregate first against the joined schemas
                let agg_result = self.aggregate.as_ref()
                    .ok_or_else(|| ExpressionError::InvalidExpression("Missing aggregate for HAVING clause".to_string()))?
                    .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
                
                // Create a new schema with just the aggregate column
                let agg_schema = Schema::new(vec![self.aggregate.as_ref().unwrap().get_return_type().clone()]);
                
                // Create a new tuple with just the aggregate result
                let agg_tuple = Tuple::new(&[agg_result], agg_schema.clone(), left_tuple.get_rid());
                
                // Now evaluate the predicate against the aggregate tuple and schema
                let pred_result = self.predicate.evaluate(&agg_tuple, &agg_schema)?;
                
                if pred_result.as_bool().unwrap_or(false) {
                    Ok(Value::new(true))
                } else {
                    Ok(Value::new(false))
                }
            }
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        match self.filter_type {
            FilterType::Where => {
                if child_idx == 0 {
                    &self.predicate
                } else {
                    panic!("Invalid child index {} for WHERE FilterExpression", child_idx)
                }
            }
            FilterType::Having => {
                match child_idx {
                    0 => self.aggregate.as_ref().unwrap(),
                    1 => &self.predicate,
                    _ => panic!("Invalid child index {} for HAVING FilterExpression", child_idx)
                }
            }
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        match self.filter_type {
            FilterType::Where => {
                if children.len() != 1 {
                    panic!("WHERE FilterExpression requires exactly 1 child, got {}", children.len());
                }
                Arc::new(Expression::Filter(FilterExpression::new_where(
                    children[0].clone(),
                    self.return_type.clone(),
                )))
            }
            FilterType::Having => {
                if children.len() != 2 {
                    panic!("HAVING FilterExpression requires exactly 2 children, got {}", children.len());
                }
                Arc::new(Expression::Filter(FilterExpression::new_having(
                    children[0].clone(),
                    children[1].clone(),
                    self.return_type.clone(),
                )))
            }
        }
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        match self.filter_type {
            FilterType::Where => {
                // Check for aggregate functions in WHERE clause
                if Self::contains_aggregate(&self.predicate) {
                    return Err(ExpressionError::InvalidExpression(
                        "Aggregate functions are not allowed in WHERE clauses".to_string(),
                    ));
                }

                // Validate the predicate
                trace!("FilterExpression: Validating WHERE predicate against schema with {} columns", schema.get_column_count());
                
                // First validate the predicate and propagate any errors
                self.predicate.validate(schema)?;
                trace!("FilterExpression: Predicate validation succeeded");
                
                // Then check return type
                let pred_type = self.predicate.get_return_type();
                if pred_type.get_type() != TypeId::Boolean {
                    trace!("FilterExpression: Predicate return type mismatch. Expected Boolean, got {:?}", pred_type.get_type());
                    return Err(ExpressionError::TypeMismatch {
                        expected: TypeId::Boolean,
                        actual: pred_type.get_type(),
                    });
                }
                trace!("FilterExpression: Validation successful");
                Ok(())
            }
            FilterType::Having => {
                // Validate the aggregate expression
                if let Some(agg) = &self.aggregate {
                    trace!("FilterExpression: Validating HAVING aggregate");
                    agg.validate(schema)?;
                    trace!("FilterExpression: Aggregate validation succeeded");
                }
                
                // Validate the predicate against the aggregate schema
                let agg_schema = Schema::new(vec![self.aggregate.as_ref().unwrap().get_return_type().clone()]);
                trace!("FilterExpression: Validating HAVING predicate against aggregate schema");
                
                // First validate the predicate and propagate any errors
                self.predicate.validate(&agg_schema)?;
                trace!("FilterExpression: Predicate validation succeeded");
                
                // Then check return type
                let pred_type = self.predicate.get_return_type();
                if pred_type.get_type() != TypeId::Boolean {
                    return Err(ExpressionError::TypeMismatch {
                        expected: TypeId::Boolean,
                        actual: pred_type.get_type(),
                    });
                }
                trace!("FilterExpression: Validation successful");
                Ok(())
            }
        }
    }
}

impl Display for FilterExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.filter_type {
            FilterType::Where => write!(f, "WHERE {}", self.predicate),
            FilterType::Having => write!(f, "HAVING {} {}", self.aggregate.as_ref().unwrap(), self.predicate),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::aggregate_expression::{AggregateExpression, AggregationType};
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::sql::execution::expressions::comparison_expression::{ComparisonExpression, ComparisonType};
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_schema() -> Schema {
        let columns = vec![
            Column::new("id", TypeId::Integer),
            Column::new("name", TypeId::VarChar),
            Column::new("age", TypeId::Integer),
            Column::new("salary", TypeId::Decimal),
        ];
        Schema::new(columns)
    }

    fn create_test_tuple() -> Tuple {
        let schema = create_test_schema();
        Tuple::new(
            &vec![
                Value::new(1),
                Value::new("John".to_string()),
                Value::new(25),
                Value::new(50000.0),
            ],
            schema,
            RID::new(0, 0),
        )
    }

    #[test]
    fn test_where_clause() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        // Create age < 20 predicate
        let age_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2, // age column index
            Column::new("age", TypeId::Integer),
            vec![],
        )));
        let twenty = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(20),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            age_col,
            twenty,
            ComparisonType::LessThan,
            vec![],
        )));

        let filter = FilterExpression::new_where(
            predicate,
            Column::new("result", TypeId::Boolean),
        );
        let result = filter.evaluate(&tuple, &schema).unwrap();
        assert!(!result.as_bool().unwrap());
    }

    #[test]
    fn test_having_clause() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        // Create COUNT(*) > 5 predicate
        let count_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(AggregationType::CountStar, vec![], Column::new("count", TypeId::Integer), "".to_string())));
        let five = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(5),
            Column::new("const", TypeId::Integer),
            vec![],
        )));
        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            count_expr.clone(),
            five,
            ComparisonType::GreaterThan,
            vec![],
        )));

        let filter = FilterExpression::new_having(
            count_expr,
            predicate,
            Column::new("result", TypeId::Integer),
        );
        let result = filter.evaluate(&tuple, &schema).unwrap();
        assert!(!result.as_bool().unwrap());
    }

    #[test]
    fn test_join_evaluation() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        // Create age > salary predicate
        let age_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            2, // age column index
            Column::new("age", TypeId::Integer),
            vec![],
        )));
        let salary_col = Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
            0,
            3, // salary column index
            Column::new("salary", TypeId::Decimal),
            vec![],
        )));
        let predicate = Arc::new(Expression::Comparison(ComparisonExpression::new(
            age_col,
            salary_col,
            ComparisonType::GreaterThan,
            vec![],
        )));

        let filter = FilterExpression::new_where(
            predicate,
            Column::new("result", TypeId::Boolean),
        );
        let result = filter.evaluate(&tuple, &schema).unwrap();
        assert!(!result.as_bool().unwrap());
    }
}
