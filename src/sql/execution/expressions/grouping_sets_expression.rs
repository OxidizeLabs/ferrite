use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum GroupingType {
    GroupingSets,
    Cube,
    Rollup,
}

#[derive(Clone, Debug, PartialEq)]
pub struct GroupingSetsExpression {
    grouping_type: GroupingType,
    groups: Vec<Vec<Arc<Expression>>>,
    return_type: Column,
}

impl GroupingSetsExpression {
    pub fn new(grouping_type: GroupingType, groups: Vec<Vec<Arc<Expression>>>, return_type: Column) -> Self {
        Self {
            grouping_type,
            groups,
            return_type,
        }
    }

    pub fn get_grouping_type(&self) -> &GroupingType {
        &self.grouping_type
    }

    pub fn get_groups(&self) -> &Vec<Vec<Arc<Expression>>> {
        &self.groups
    }
    
    // Helper method to get all expressions flattened into a single vector
    pub fn get_all_expressions(&self) -> Vec<Arc<Expression>> {
        let mut result = Vec::new();
        for group in &self.groups {
            for expr in group {
                result.push(expr.clone());
            }
        }
        result
    }
}

impl ExpressionOps for GroupingSetsExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        // GroupingSets evaluation typically happens at a higher level in the execution engine
        // This method should not be called directly on a GroupingSetsExpression
        Err(ExpressionError::InvalidOperation(
            "GroupingSetsExpression cannot be evaluated directly".to_string()
        ))
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        // GroupingSets evaluation typically happens at a higher level in the execution engine
        Err(ExpressionError::InvalidOperation(
            "GroupingSetsExpression cannot be evaluated directly in a join".to_string()
        ))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        // Flatten all expressions in all groups into a single vector
        let all_exprs = self.get_all_expressions();
        if child_idx < all_exprs.len() {
            &all_exprs[child_idx]
        } else {
            panic!("Child index out of bounds for GroupingSetsExpression")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        // This is a bit tricky since we have nested vectors
        // We'll return an empty vector since the actual children are in the groups
        // Use get_all_expressions() to get a flattened view of all expressions
        static EMPTY: Vec<Arc<Expression>> = Vec::new();
        &EMPTY
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        // This is complex for GroupingSets since we have nested groups
        // For simplicity, we'll just create a new expression with the same structure
        // but replace the expressions in order
        
        let mut new_groups = Vec::new();
        let mut child_idx = 0;
        
        for group in &self.groups {
            let mut new_group = Vec::new();
            for _ in group {
                if child_idx < children.len() {
                    new_group.push(children[child_idx].clone());
                    child_idx += 1;
                }
            }
            new_groups.push(new_group);
        }
        
        Arc::new(Expression::GroupingSets(GroupingSetsExpression::new(
            self.grouping_type.clone(),
            new_groups,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all expressions in all groups
        for group in &self.groups {
            for expr in group {
                expr.validate(schema)?;
            }
        }
        Ok(())
    }
}

impl Display for GroupingSetsExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.grouping_type {
            GroupingType::GroupingSets => write!(f, "GROUPING SETS ("),
            GroupingType::Cube => write!(f, "CUBE ("),
            GroupingType::Rollup => write!(f, "ROLLUP ("),
        }?;

        let groups: Vec<String> = self.groups.iter()
            .map(|group| {
                let exprs: Vec<String> = group.iter()
                    .map(|e| e.to_string())
                    .collect();
                format!("({})", exprs.join(", "))
            })
            .collect();

        write!(f, "{})", groups.join(", "))
    }
} 