use crate::types_db::type_id::TypeId;
use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use rand::{Rng, SeedableRng};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use crate::types_db::types::Type;

#[derive(Clone, Debug, PartialEq)]
pub struct RandomExpression {
    seed: Option<Arc<Expression>>,  // Optional seed expression
    return_type: Column,
}

impl RandomExpression {
    pub fn new(seed: Option<Arc<Expression>>, return_type: Column) -> Self {
        Self {
            seed,
            return_type,
        }
    }
}

impl ExpressionOps for RandomExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        if let Some(seed_expr) = &self.seed {
            // If seed is provided, use it to initialize the RNG
            let seed = seed_expr.evaluate(tuple, schema)?;
            let seed_val = seed.as_bigint().or_else(|_| {
                Err(ExpressionError::InvalidSeed("Seed must be convertible to integer".to_string()))
            })? as u64;
            let mut rng = rand::rngs::StdRng::seed_from_u64(seed_val);
            Ok(Value::new(rng.random::<f64>()))
        } else {
            // Otherwise use thread RNG
            let mut rng = rand::rng();
            Ok(Value::new(rng.random::<f64>()))
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // For join evaluation, we'll evaluate using the left tuple and schema
        self.evaluate(left_tuple, left_schema)
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if let Some(seed) = &self.seed {
            if child_idx == 0 {
                seed
            } else {
                panic!("Index out of bounds: RandomExpression has at most one child")
            }
        } else {
            panic!("Index out of bounds: RandomExpression has no children")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        // This is a bit of a hack since we don't store the seed in a Vec
        // but it's required by the trait. In practice, this method
        // shouldn't be called directly - use get_child_at instead
        static EMPTY: Vec<Arc<Expression>> = Vec::new();
        match &self.seed {
            Some(_) => panic!("Use get_child_at for RandomExpression"),
            None => &EMPTY,
        }
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        assert!(children.len() <= 1, "RandomExpression accepts at most one child");
        Arc::new(Expression::Random(RandomExpression::new(
            children.into_iter().next(),
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate the seed expression if it exists
        if let Some(seed) = &self.seed {
            seed.validate(schema)?;
            
            // Check that seed expression returns a type that can be converted to integer
            let seed_type = seed.get_return_type().get_type();
            match seed_type {
                TypeId::TinyInt | TypeId::SmallInt | TypeId::Integer | TypeId::BigInt => Ok(()),
                _ => Err(ExpressionError::InvalidSeed(
                    "Seed expression must return an integer type".to_string()
                ))
            }
        } else {
            Ok(())
        }
    }
}

impl Display for RandomExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if let Some(seed) = &self.seed {
            write!(f, "RANDOM({})", seed)
        } else {
            write!(f, "RANDOM()")
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::common::rid::RID;
    use super::*;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("col1", TypeId::Integer),
            Column::new("col2", TypeId::VarChar),
        ])
    }

    fn create_test_tuple() -> (Tuple, Schema) {
        let schema = create_test_schema();
        let values = vec![Value::new(42), Value::new("test")];
        let tuple = Tuple::new(&values, schema.clone(), RID::new(1, 1));
        (tuple, schema)
    }

    #[test]
    fn test_unseeded_random() {
        let expr = RandomExpression::new(
            None,
            Column::new("random", TypeId::Decimal)
        );
        
        let (tuple, schema) = create_test_tuple();
        
        // Test multiple evaluations
        for _ in 0..10 {
            let result = expr.evaluate(&tuple, &schema).unwrap();
            let value = result.as_decimal().unwrap();
            assert!(value >= 0.0 && value < 1.0);
        }
    }

    #[test]
    fn test_seeded_random() {
        // Create a constant seed expression
        let seed_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42i64),
            Column::new("seed", TypeId::BigInt),
            vec![]
        )));

        let expr = RandomExpression::new(
            Some(seed_expr),
            Column::new("random", TypeId::Decimal)
        );
        
        let (tuple, schema) = create_test_tuple();
        
        // Same seed should produce same sequence
        let first_sequence: Vec<f64> = (0..3)
            .map(|_| expr.evaluate(&tuple, &schema).unwrap().as_decimal().unwrap())
            .collect();
            
        let second_sequence: Vec<f64> = (0..3)
            .map(|_| expr.evaluate(&tuple, &schema).unwrap().as_decimal().unwrap())
            .collect();
            
        assert_eq!(first_sequence, second_sequence);
    }

    #[test]
    fn test_invalid_seed_type() {
        // Create a constant seed expression with invalid type (VARCHAR)
        let seed_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("not a number"),
            Column::new("seed", TypeId::VarChar),
            vec![]
        )));

        let expr = RandomExpression::new(
            Some(seed_expr),
            Column::new("random", TypeId::Decimal)
        );
        
        let (tuple, schema) = create_test_tuple();
        
        // Validation should fail
        assert!(matches!(
            expr.validate(&schema),
            Err(ExpressionError::InvalidSeed(_))
        ));
        
        // Evaluation should fail
        assert!(matches!(
            expr.evaluate(&tuple, &schema),
            Err(ExpressionError::InvalidSeed(_))
        ));
    }

    #[test]
    fn test_display() {
        // Test unseeded random
        let expr = RandomExpression::new(
            None,
            Column::new("random", TypeId::Decimal)
        );
        assert_eq!(expr.to_string(), "RANDOM()");

        // Test seeded random
        let seed_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42i64),
            Column::new("seed", TypeId::BigInt),
            vec![]
        )));
        let expr = RandomExpression::new(
            Some(seed_expr),
            Column::new("random", TypeId::Decimal)
        );
        assert_eq!(expr.to_string(), "RANDOM(42)");
    }

    #[test]
    fn test_clone_with_children() {
        let expr = RandomExpression::new(
            None,
            Column::new("random", TypeId::Decimal)
        );
        
        // Test cloning with no children
        let cloned = expr.clone_with_children(vec![]);
        assert!(matches!(cloned.as_ref(), Expression::Random(_)));
        
        // Test cloning with one child
        let seed_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42i64),
            Column::new("seed", TypeId::BigInt),
            vec![]
        )));
        let cloned = expr.clone_with_children(vec![seed_expr.clone()]);
        assert!(matches!(cloned.as_ref(), Expression::Random(_)));
        
        // Test that cloning with too many children panics
        let seed_expr2 = seed_expr.clone();
        assert!(std::panic::catch_unwind(|| {
            expr.clone_with_children(vec![seed_expr, seed_expr2]);
        }).is_err());
    }
} 