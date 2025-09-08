use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::value::{Val, Value};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
pub struct CaseExpression {
    /// Optional base expression for CASE expr WHEN ... form
    base_expr: Option<Arc<Expression>>,
    /// WHEN expressions
    when_exprs: Vec<Arc<Expression>>,
    /// THEN expressions
    then_exprs: Vec<Arc<Expression>>,
    /// Optional ELSE expression
    else_expr: Option<Arc<Expression>>,
    /// Return type of the CASE expression
    ret_type: Column,
    /// All child expressions
    children: Vec<Arc<Expression>>,
}

impl CaseExpression {
    pub fn new(
        base_expr: Option<Arc<Expression>>,
        when_exprs: Vec<Arc<Expression>>,
        then_exprs: Vec<Arc<Expression>>,
        else_expr: Option<Arc<Expression>>,
    ) -> Result<Self, String> {
        if when_exprs.len() != then_exprs.len() {
            return Err("Number of WHEN and THEN expressions must match".to_string());
        }
        if when_exprs.is_empty() {
            return Err("CASE expression must have at least one WHEN clause".to_string());
        }

        // Determine return type from THEN expressions and ELSE expression
        let ret_type = Self::determine_return_type(&then_exprs, else_expr.as_ref())?;

        // Collect all child expressions
        let mut children = Vec::new();
        if let Some(base) = base_expr.as_ref() {
            children.push(base.clone());
        }
        children.extend(when_exprs.iter().cloned());
        children.extend(then_exprs.iter().cloned());
        if let Some(else_expr) = else_expr.as_ref() {
            children.push(else_expr.clone());
        }

        Ok(Self {
            base_expr,
            when_exprs,
            then_exprs,
            else_expr,
            ret_type,
            children,
        })
    }

    fn determine_return_type(
        then_exprs: &[Arc<Expression>],
        else_expr: Option<&Arc<Expression>>,
    ) -> Result<Column, String> {
        let mut types = then_exprs
            .iter()
            .map(|expr| expr.get_return_type().get_type())
            .collect::<Vec<_>>();

        if let Some(else_expr) = else_expr {
            types.push(else_expr.get_return_type().get_type());
        }

        // All result expressions must have compatible types
        let common_type = types.first().ok_or("No result expressions found")?;
        for typ in types.iter().skip(1) {
            if typ != common_type {
                return Err(format!(
                    "Incompatible types in CASE expression: {:?} and {:?}",
                    common_type, typ
                ));
            }
        }

        Ok(Column::new("case_result", *common_type))
    }
}

impl ExpressionOps for CaseExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        use log::debug;

        // Debug: show the tuple being processed
        let tuple_data: Vec<String> = (0..schema.get_column_count() as usize)
            .map(|i| tuple.get_value(i).to_string())
            .collect();
        debug!("CASE expression evaluating tuple: {:?}", tuple_data);

        // For CASE expr WHEN ... form, evaluate base expression
        let base_value = match &self.base_expr {
            Some(expr) => Some(expr.evaluate(tuple, schema)?),
            None => None,
        };

        // Evaluate each WHEN condition until we find a match
        for (i, when_expr) in self.when_exprs.iter().enumerate() {
            let when_value = when_expr.evaluate(tuple, schema)?;
            let matches = match &base_value {
                // For CASE expr WHEN ... form, compare base_value with when_value
                Some(base) => base == &when_value,
                // For CASE WHEN ... form, check if when_value is true
                None => match when_value.get_val() {
                    Val::Boolean(b) => {
                        debug!(
                            "CASE WHEN condition {} evaluated to: {} for tuple {:?}",
                            i, b, tuple_data
                        );
                        *b
                    }
                    _ => return Err(ExpressionError::InvalidType("Expected boolean".to_string())),
                },
            };

            if matches {
                debug!(
                    "CASE WHEN condition {} matched, evaluating THEN expression for tuple {:?}",
                    i, tuple_data
                );
                // Return a corresponding THEN result
                let result = self.then_exprs[i].evaluate(tuple, schema)?;
                debug!(
                    "CASE WHEN condition {} THEN result: {} for tuple {:?}",
                    i, result, tuple_data
                );
                return Ok(result);
            }
        }

        debug!(
            "No CASE WHEN conditions matched, evaluating ELSE expression for tuple {:?}",
            tuple_data
        );
        // If no conditions matched, return ELSE result or NULL
        match &self.else_expr {
            Some(expr) => {
                let result = expr.evaluate(tuple, schema)?;
                debug!("CASE ELSE result: {} for tuple {:?}", result, tuple_data);
                Ok(result)
            }
            None => Ok(Value::new(Val::Null)),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // Similar to evaluate but with join context
        let base_value = match &self.base_expr {
            Some(expr) => {
                Some(expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?)
            }
            None => None,
        };

        for (i, when_expr) in self.when_exprs.iter().enumerate() {
            let when_value =
                when_expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema)?;
            let matches = match &base_value {
                Some(base) => base == &when_value,
                None => match when_value.get_val() {
                    Val::Boolean(b) => *b,
                    _ => return Err(ExpressionError::InvalidType("Expected boolean".to_string())),
                },
            };

            if matches {
                return self.then_exprs[i].evaluate_join(
                    left_tuple,
                    left_schema,
                    right_tuple,
                    right_schema,
                );
            }
        }

        match &self.else_expr {
            Some(expr) => expr.evaluate_join(left_tuple, left_schema, right_tuple, right_schema),
            None => Ok(Value::new(Val::Null)),
        }
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
        let mut offset;

        // Extract base expression if present
        let (base_expr, base_offset) = match &self.base_expr {
            Some(_) => (Some(children[0].clone()), 1),
            None => (None, 0),
        };
        offset = base_offset;

        // Extract WHEN and THEN expressions
        let when_count = self.when_exprs.len();
        let when_exprs = children[offset..offset + when_count].to_vec();
        offset += when_count;
        let then_exprs = children[offset..offset + when_count].to_vec();
        offset += when_count;

        // Extract ELSE expression if present
        let else_expr = if offset < children.len() {
            Some(children[offset].clone())
        } else {
            None
        };

        Arc::new(Expression::Case(
            Self::new(base_expr, when_exprs, then_exprs, else_expr)
                .expect("Invalid children for CASE expression"),
        ))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate base expression if present
        if let Some(expr) = &self.base_expr {
            expr.validate(schema)?;
        }

        // Validate all WHEN expressions
        for when_expr in &self.when_exprs {
            when_expr.validate(schema)?;
            // Ensure WHEN expressions return boolean (for CASE WHEN form)
            if self.base_expr.is_none() {
                let when_type = when_expr.get_return_type().get_type();
                if when_type != TypeId::Boolean {
                    return Err(ExpressionError::InvalidType(
                        "WHEN expression must return boolean".to_string(),
                    ));
                }
            }
        }

        // Validate all THEN expressions
        for then_expr in &self.then_exprs {
            then_expr.validate(schema)?;
        }

        // Validate ELSE expression if present
        if let Some(expr) = &self.else_expr {
            expr.validate(schema)?;
        }

        Ok(())
    }
}

impl Display for CaseExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "CASE")?;

        // Format base expression if present
        if let Some(base) = &self.base_expr {
            if f.alternate() {
                write!(f, " {:#}", base)?;
            } else {
                write!(f, " {}", base)?;
            }
        }

        // Format WHEN/THEN pairs
        for (when, then) in self.when_exprs.iter().zip(self.then_exprs.iter()) {
            if f.alternate() {
                write!(f, " WHEN {:#} THEN {:#}", when, then)?;
            } else {
                write!(f, " WHEN {} THEN {}", when, then)?;
            }
        }

        // Format ELSE clause if present
        if let Some(else_expr) = &self.else_expr {
            if f.alternate() {
                write!(f, " ELSE {:#}", else_expr)?;
            } else {
                write!(f, " ELSE {}", else_expr)?;
            }
        }

        write!(f, " END")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;

    #[test]
    fn test_simple_case() {
        let when_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(true),
            Column::new("when", TypeId::Boolean),
            vec![],
        )));
        let then_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("then", TypeId::Integer),
            vec![],
        )));
        let else_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(0),
            Column::new("else", TypeId::Integer),
            vec![],
        )));

        let case =
            CaseExpression::new(None, vec![when_expr], vec![then_expr], Some(else_expr)).unwrap();

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = case.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new(42));
    }

    #[test]
    fn test_case_with_base() {
        let base_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("base", TypeId::Integer),
            vec![],
        )));
        let when_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(1),
            Column::new("when", TypeId::Integer),
            vec![],
        )));
        let then_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new("match"),
            Column::new("then", TypeId::VarChar),
            vec![],
        )));

        let case =
            CaseExpression::new(Some(base_expr), vec![when_expr], vec![then_expr], None).unwrap();

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = case.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result, Value::new("match"));
    }

    #[test]
    fn test_case_no_match() {
        let when_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(false),
            Column::new("when", TypeId::Boolean),
            vec![],
        )));
        let then_expr = Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(42),
            Column::new("then", TypeId::Integer),
            vec![],
        )));

        let case = CaseExpression::new(None, vec![when_expr], vec![then_expr], None).unwrap();

        let schema = Schema::new(vec![]);
        let tuple = Tuple::new(&*vec![], &schema, RID::new(0, 0));

        let result = case.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Null);
    }
}
