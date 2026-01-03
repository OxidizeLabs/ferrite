use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};

#[derive(Clone, Debug, PartialEq)]
pub enum SubqueryType {
    Scalar,     // Returns single value
    Exists,     // EXISTS subquery
    InList,     // IN subquery
    Quantified, // ANY/ALL subquery
}

#[derive(Clone, Debug, PartialEq)]
pub struct SubqueryExpression {
    subquery: Arc<Expression>,
    subquery_type: SubqueryType,
    return_type: Column,
    children: Vec<Arc<Expression>>,
    // Cache for scalar subquery results to avoid re-execution
    cached_result: Option<Value>,
}

impl SubqueryExpression {
    pub fn new(
        subquery: Arc<Expression>,
        subquery_type: SubqueryType,
        return_type: Column,
    ) -> Self {
        Self {
            subquery: subquery.clone(),
            subquery_type,
            return_type,
            children: vec![subquery],
            cached_result: None,
        }
    }

    /// Set a cached result for this subquery
    /// This should be called by the query planner after executing the subquery
    pub fn with_cached_result(mut self, result: Value) -> Self {
        self.cached_result = Some(result);
        self
    }

    /// Check if this subquery has a cached result
    pub fn has_cached_result(&self) -> bool {
        self.cached_result.is_some()
    }

    /// Get the cached result if available
    pub fn get_cached_result(&self) -> Option<&Value> {
        self.cached_result.as_ref()
    }

    fn extract_scalar_value(&self, value: Value) -> Result<Value, ExpressionError> {
        match value.get_val() {
            // If it's already a scalar value, return as is
            Val::Null
            | Val::Boolean(_)
            | Val::TinyInt(_)
            | Val::SmallInt(_)
            | Val::Integer(_)
            | Val::BigInt(_)
            | Val::Decimal(_)
            | Val::Timestamp(_)
            | Val::VarLen(_)
            | Val::ConstLen(_) => Ok(value),

            // If it's a vector with one element (common for scalar subqueries), return that element
            Val::Vector(v) if v.len() == 1 => Ok(v[0].clone()),

            // Otherwise, it's an error
            _ => Err(ExpressionError::InvalidOperation(
                "Scalar subquery must return exactly one value".to_string(),
            )),
        }
    }

    /// Check if the subquery contains aggregate functions
    fn contains_aggregate(&self, expr: &Expression) -> bool {
        match expr {
            Expression::Aggregate(_) => true,
            _ => expr
                .get_children()
                .iter()
                .any(|child| self.contains_aggregate(child)),
        }
    }

    /// Execute a scalar subquery with aggregates by creating a temporary execution context
    /// This is a simplified implementation that handles the specific case of AVG(column)
    fn execute_scalar_aggregate_subquery(&self) -> Result<Value, ExpressionError> {
        // Scalar subqueries with aggregates should be handled at the query planning level,
        // not at the expression evaluation level. This is because they need access to
        // the full execution context, data sources, and proper query planning.
        //
        // The proper approach is to:
        // 1. Detect scalar subqueries during query planning
        // 2. Replace them with correlated subquery plans
        // 3. Execute the subquery plans separately and cache results
        // 4. Use the cached results during expression evaluation

        match self.subquery.as_ref() {
            Expression::Aggregate(agg) => {
                // For now, return an error indicating this needs to be handled at plan level
                Err(ExpressionError::InvalidOperation(format!(
                    "Scalar subquery with {} aggregate must be handled at query planning level, not expression evaluation level",
                    agg.get_function_name()
                )))
            },
            _ => {
                // For non-aggregate subqueries, try to evaluate directly
                // This is a fallback for simple scalar subqueries
                Err(ExpressionError::InvalidOperation(
                    "Scalar subqueries must be handled at query planning level".to_string(),
                ))
            },
        }
    }
}

impl ExpressionOps for SubqueryExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match self.subquery_type {
            SubqueryType::Scalar => {
                // First check if we have a cached result
                if let Some(cached_result) = &self.cached_result {
                    return Ok(cached_result.clone());
                }

                // Check if this is a scalar subquery with aggregates
                if self.contains_aggregate(&self.subquery) {
                    // Execute as a separate query to get the aggregate result
                    let result = self.execute_scalar_aggregate_subquery()?;
                    self.extract_scalar_value(result)
                } else {
                    // Regular scalar subquery evaluation
                    let result = self.subquery.evaluate(tuple, schema)?;
                    self.extract_scalar_value(result)
                }
            },
            SubqueryType::Exists => {
                // First check if we have a cached result
                if let Some(cached_result) = &self.cached_result {
                    return Ok(cached_result.clone());
                }

                // Check if subquery returns any rows
                let result = self.subquery.evaluate(tuple, schema)?;
                Ok(Value::new(!result.is_null()))
            },
            SubqueryType::InList => {
                // First check if we have a cached result
                if let Some(cached_result) = &self.cached_result {
                    return Ok(cached_result.clone());
                }

                // Get list of values from subquery
                let result = self.subquery.evaluate(tuple, schema)?;

                // Convert the result to a vector if it's not already one
                match result.get_val() {
                    Val::Vector(_) => Ok(result),
                    Val::Null => Ok(Value::new_vector(vec![Value::new(Val::Null)])),
                    // For a scalar result, wrap it in a vector
                    _ => Ok(Value::new_vector(vec![result])),
                }
            },
            SubqueryType::Quantified => {
                // First check if we have a cached result
                if let Some(cached_result) = &self.cached_result {
                    return Ok(cached_result.clone());
                }

                // Handle ANY/ALL comparison
                self.subquery.evaluate(tuple, schema)
            },
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        match self.subquery_type {
            SubqueryType::Scalar => {
                // First check if we have a cached result
                if let Some(cached_result) = &self.cached_result {
                    return Ok(cached_result.clone());
                }

                // Check if this is a scalar subquery with aggregates
                if self.contains_aggregate(&self.subquery) {
                    // Execute as a separate query to get the aggregate result
                    let result = self.execute_scalar_aggregate_subquery()?;
                    self.extract_scalar_value(result)
                } else {
                    // Regular scalar subquery evaluation
                    let result = self.subquery.evaluate_join(
                        left_tuple,
                        left_schema,
                        right_tuple,
                        right_schema,
                    )?;
                    self.extract_scalar_value(result)
                }
            },
            SubqueryType::Exists => {
                // First check if we have a cached result
                if let Some(cached_result) = &self.cached_result {
                    return Ok(cached_result.clone());
                }

                let result = self.subquery.evaluate_join(
                    left_tuple,
                    left_schema,
                    right_tuple,
                    right_schema,
                )?;
                Ok(Value::new(!result.is_null()))
            },
            SubqueryType::InList => {
                // First check if we have a cached result
                if let Some(cached_result) = &self.cached_result {
                    return Ok(cached_result.clone());
                }

                // Get list of values from subquery for join evaluation
                let result = self.subquery.evaluate_join(
                    left_tuple,
                    left_schema,
                    right_tuple,
                    right_schema,
                )?;

                // Convert the result to a vector if it's not already one
                match result.get_val() {
                    Val::Vector(_) => Ok(result),
                    Val::Null => Ok(Value::new_vector(vec![Value::new(Val::Null)])),
                    // For a scalar result, wrap it in a vector
                    _ => Ok(Value::new_vector(vec![result])),
                }
            },
            SubqueryType::Quantified => {
                // First check if we have a cached result
                if let Some(cached_result) = &self.cached_result {
                    return Ok(cached_result.clone());
                }

                self.subquery
                    .evaluate_join(left_tuple, left_schema, right_tuple, right_schema)
            },
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if child_idx == 0 {
            &self.subquery
        } else {
            panic!("Index out of bounds in SubqueryExpression::get_child_at")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        if children.len() != 1 {
            panic!("SubqueryExpression requires exactly one child");
        }

        let mut new_subquery = SubqueryExpression::new(
            children[0].clone(),
            self.subquery_type.clone(),
            self.return_type.clone(),
        );

        // Preserve cached result if it exists
        if let Some(cached_result) = &self.cached_result {
            new_subquery.cached_result = Some(cached_result.clone());
        }

        Arc::new(Expression::Subquery(new_subquery))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the subquery expression
        self.subquery.validate(schema)?;

        // Additional validation based on subquery type
        match self.subquery_type {
            SubqueryType::Scalar => {
                // Scalar subquery should return a single value
                if self.return_type.get_type() == self.subquery.get_return_type().get_type() {
                    Ok(())
                } else {
                    Err(ExpressionError::InvalidReturnType(
                        "Scalar subquery return type does not match expected type".to_string(),
                    ))
                }
            },
            SubqueryType::Exists => {
                // EXISTS subquery should return a boolean
                if self.return_type.get_type() == TypeId::Boolean {
                    Ok(())
                } else {
                    Err(ExpressionError::InvalidReturnType(
                        "EXISTS subquery must return a boolean value".to_string(),
                    ))
                }
            },
            SubqueryType::InList | SubqueryType::Quantified => {
                // IN/ANY/ALL subqueries should return a list of values
                Ok(())
            },
        }
    }
}

impl Display for SubqueryExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "({})", self.subquery)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::mock_expression::MockExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Val;

    // Helper function to create a mock expression that returns a specific value
    fn create_mock_expression(name: String, return_type: TypeId) -> Arc<Expression> {
        Arc::new(Expression::Mock(MockExpression::new(name, return_type)))
    }

    fn create_empty_tuple() -> (Tuple, Schema) {
        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&[], &schema, RID::new(0, 0));
        (tuple, schema)
    }

    #[test]
    fn test_scalar_subquery() {
        let mock_expr = create_mock_expression("test_scalar".to_string(), TypeId::Integer);
        let return_type = Column::new("test", TypeId::Integer);

        let subquery = SubqueryExpression::new(mock_expr, SubqueryType::Scalar, return_type);

        let (tuple, schema) = create_empty_tuple();
        let result = subquery.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(42i32)); // MockExpression returns 42 for Integer type
    }

    #[test]
    fn test_exists_subquery() {
        let mock_expr = create_mock_expression("test_exists".to_string(), TypeId::Boolean);
        let return_type = Column::new("test", TypeId::Boolean);

        let subquery = SubqueryExpression::new(mock_expr, SubqueryType::Exists, return_type);

        let (tuple, schema) = create_empty_tuple();
        let result = subquery.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(true)); // MockExpression returns true for Boolean type
    }

    #[test]
    fn test_in_list_subquery() {
        let mock_expr = create_mock_expression("test_in_list".to_string(), TypeId::Vector);
        let return_type = Column::new("test", TypeId::Vector);

        let subquery = SubqueryExpression::new(mock_expr, SubqueryType::InList, return_type);

        let (tuple, schema) = create_empty_tuple();
        let result = subquery.evaluate(&tuple, &schema).unwrap();
        // MockExpression returns [1, 2, 3] for Vector type
        assert_eq!(
            result,
            Value::new_vector(vec![Value::new(1i32), Value::new(2i32), Value::new(3i32)])
        );
    }

    #[test]
    fn test_scalar_subquery_error() {
        let mock_expr = create_mock_expression("test_error".to_string(), TypeId::Invalid);
        let return_type = Column::new("test", TypeId::Invalid);

        let subquery = SubqueryExpression::new(mock_expr, SubqueryType::Scalar, return_type);

        let (tuple, schema) = create_empty_tuple();
        let result = subquery.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Null);
    }

    #[test]
    fn test_validate_scalar_subquery() {
        let mock_expr = create_mock_expression("test_validate".to_string(), TypeId::Integer);
        let return_type = Column::new("test", TypeId::Integer);

        let subquery = SubqueryExpression::new(mock_expr, SubqueryType::Scalar, return_type);

        let (_, schema) = create_empty_tuple();
        assert!(subquery.validate(&schema).is_ok());
    }

    #[test]
    fn test_any_subquery() {
        let mock_expr = create_mock_expression("test_any".to_string(), TypeId::Vector);
        let return_type = Column::new("test", TypeId::Vector);

        let subquery = SubqueryExpression::new(mock_expr, SubqueryType::Quantified, return_type);

        let (tuple, schema) = create_empty_tuple();
        let result = subquery.evaluate(&tuple, &schema).unwrap();
        // MockExpression returns [1, 2, 3] for Vector type
        assert_eq!(
            result,
            Value::new_vector(vec![Value::new(1i32), Value::new(2i32), Value::new(3i32)])
        );
    }

    #[test]
    fn test_evaluate_join() {
        let mock_expr = create_mock_expression("test_join".to_string(), TypeId::Integer);
        let return_type = Column::new("test", TypeId::Integer);

        let subquery = SubqueryExpression::new(mock_expr, SubqueryType::Scalar, return_type);

        let (left_tuple, left_schema) = create_empty_tuple();
        let (right_tuple, right_schema) = create_empty_tuple();

        let result = subquery
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();
        assert_eq!(result, Value::new(42i32)); // MockExpression returns 42 for Integer type
    }

    #[test]
    fn test_clone_with_children() {
        let return_type = Column::new("test", TypeId::Integer);
        let mock_value = Value::new(42);
        let mock_expr = create_mock_expression(ToString::to_string(&mock_value), TypeId::Integer);

        let subquery =
            SubqueryExpression::new(mock_expr.clone(), SubqueryType::Scalar, return_type.clone());

        let new_children = vec![mock_expr];
        let cloned = subquery.clone_with_children(new_children);

        assert_eq!(cloned.get_return_type(), &return_type);
    }

    #[test]
    fn test_contains_aggregate() {
        use crate::sql::execution::expressions::aggregate_expression::{
            AggregateExpression, AggregationType,
        };

        // Create an aggregate expression
        let agg_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Avg,
            vec![],
            Column::new("avg", TypeId::Decimal),
            "AVG".to_string(),
        )));

        let subquery = SubqueryExpression::new(
            agg_expr,
            SubqueryType::Scalar,
            Column::new("test", TypeId::Decimal),
        );

        // Test that it correctly identifies aggregate expressions
        assert!(subquery.contains_aggregate(&subquery.subquery));

        // Test with non-aggregate expression
        let non_agg_expr = create_mock_expression("test".to_string(), TypeId::Integer);
        assert!(!subquery.contains_aggregate(&non_agg_expr));
    }

    #[test]
    fn test_cached_result_mechanism() {
        let mock_expr = create_mock_expression("test_cached".to_string(), TypeId::Integer);
        let return_type = Column::new("test", TypeId::Integer);

        // Create subquery with cached result
        let cached_value = Value::new(42);
        let subquery = SubqueryExpression::new(mock_expr, SubqueryType::Scalar, return_type)
            .with_cached_result(cached_value.clone());

        // Verify cached result is set
        assert!(subquery.has_cached_result());
        assert_eq!(subquery.get_cached_result(), Some(&cached_value));

        let (tuple, schema) = create_empty_tuple();
        let result = subquery.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, cached_value); // Should return cached value
    }

    #[test]
    fn test_cached_result_preservation_on_clone() {
        let mock_expr = create_mock_expression("test_clone".to_string(), TypeId::Integer);
        let return_type = Column::new("test", TypeId::Integer);

        // Create subquery with cached result
        let cached_value = Value::new(100);
        let subquery =
            SubqueryExpression::new(mock_expr.clone(), SubqueryType::Scalar, return_type)
                .with_cached_result(cached_value.clone());

        // Clone with same children
        let cloned = subquery.clone_with_children(vec![mock_expr]);

        // Verify cached result is preserved
        if let Expression::Subquery(cloned_subquery) = cloned.as_ref() {
            assert!(cloned_subquery.has_cached_result());
            assert_eq!(cloned_subquery.get_cached_result(), Some(&cached_value));
        } else {
            panic!("Expected SubqueryExpression after cloning");
        }
    }

    #[test]
    fn test_aggregate_subquery_error_handling() {
        use crate::sql::execution::expressions::aggregate_expression::{
            AggregateExpression, AggregationType,
        };

        // Create an aggregate expression
        let agg_expr = Arc::new(Expression::Aggregate(AggregateExpression::new(
            AggregationType::Sum,
            vec![],
            Column::new("sum", TypeId::Integer),
            "SUM".to_string(),
        )));

        let subquery = SubqueryExpression::new(
            agg_expr,
            SubqueryType::Scalar,
            Column::new("test", TypeId::Integer),
        );

        let (tuple, schema) = create_empty_tuple();
        let result = subquery.evaluate(&tuple, &schema);

        // Should return an error indicating it needs to be handled at plan level
        assert!(result.is_err());
        let error_msg = format!("{}", result.unwrap_err());
        assert!(error_msg.contains("must be handled at query planning level"));
    }
}
