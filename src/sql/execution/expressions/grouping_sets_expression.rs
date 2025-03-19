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
    children: Vec<Arc<Expression>>, // Flattened list of all expressions for standard pattern
}

impl GroupingSetsExpression {
    pub fn new(
        grouping_type: GroupingType,
        groups: Vec<Vec<Arc<Expression>>>,
        return_type: Column,
    ) -> Self {
        // Flatten all expressions into a single children vector
        let mut children = Vec::new();
        for group in &groups {
            for expr in group {
                children.push(expr.clone());
            }
        }

        Self {
            grouping_type,
            groups,
            return_type,
            children,
        }
    }

    pub fn get_grouping_type(&self) -> &GroupingType {
        &self.grouping_type
    }

    pub fn get_groups(&self) -> &Vec<Vec<Arc<Expression>>> {
        &self.groups
    }
}

impl ExpressionOps for GroupingSetsExpression {
    fn evaluate(&self, _tuple: &Tuple, _schema: &Schema) -> Result<Value, ExpressionError> {
        // GroupingSets evaluation typically happens at a higher level in the execution engine
        // This method should not be called directly on a GroupingSetsExpression
        Err(ExpressionError::InvalidOperation(
            "GroupingSetsExpression cannot be evaluated directly".to_string(),
        ))
    }

    fn evaluate_join(
        &self,
        _left_tuple: &Tuple,
        _left_schema: &Schema,
        _right_tuple: &Tuple,
        _right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        // GroupingSets evaluation typically happens at a higher level in the execution engine
        Err(ExpressionError::InvalidOperation(
            "GroupingSetsExpression cannot be evaluated directly in a join".to_string(),
        ))
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if child_idx < self.children.len() {
            &self.children[child_idx]
        } else {
            panic!("Child index out of bounds for GroupingSetsExpression")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.children
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        // Reconstruct the groups structure with the new children
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
            if !new_group.is_empty() {
                new_groups.push(new_group);
            }
        }

        Arc::new(Expression::GroupingSets(GroupingSetsExpression::new(
            self.grouping_type.clone(),
            new_groups,
            self.return_type.clone(),
        )))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all child expressions
        for child in &self.children {
            child.validate(schema)?;
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

        let groups: Vec<String> = self
            .groups
            .iter()
            .map(|group| {
                let exprs: Vec<String> = group.iter().map(|e| e.to_string()).collect();
                format!("({})", exprs.join(", "))
            })
            .collect();

        write!(f, "{})", groups.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::common::rid::RID;
    use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::Value;

    fn create_test_expression(value: i32) -> Arc<Expression> {
        let column = Column::new(&format!("const_{}", value), TypeId::Integer);
        Arc::new(Expression::Constant(ConstantExpression::new(
            Value::new(value),
            column,
            vec![],
        )))
    }

    #[test]
    fn test_grouping_sets_creation() {
        // Create test expressions
        let expr1 = create_test_expression(1);
        let expr2 = create_test_expression(2);
        let expr3 = create_test_expression(3);

        // Create groups
        let group1 = vec![expr1.clone(), expr2.clone()];
        let group2 = vec![expr2.clone(), expr3.clone()];
        let group3 = vec![expr1.clone(), expr3.clone()];

        let groups = vec![group1, group2, group3];
        let return_type = Column::new("grouping_sets", TypeId::Vector);

        // Create GroupingSetsExpression
        let expr =
            GroupingSetsExpression::new(GroupingType::GroupingSets, groups, return_type.clone());

        // Verify properties
        assert_eq!(expr.get_grouping_type(), &GroupingType::GroupingSets);
        assert_eq!(expr.get_groups().len(), 3);
        assert_eq!(expr.get_return_type().get_name(), "grouping_sets");
        assert_eq!(expr.get_return_type().get_type(), TypeId::Vector);

        // Verify children are properly flattened
        assert_eq!(expr.get_children().len(), 6); // 2 + 2 + 2 expressions
    }

    #[test]
    fn test_children_access() {
        // Create test expressions
        let expr1 = create_test_expression(1);
        let expr2 = create_test_expression(2);
        let expr3 = create_test_expression(3);

        // Create groups
        let group1 = vec![expr1.clone(), expr2.clone()];
        let group2 = vec![expr3.clone()];

        let groups = vec![group1, group2];
        let return_type = Column::new("grouping_sets", TypeId::Vector);

        // Create GroupingSetsExpression
        let expr =
            GroupingSetsExpression::new(GroupingType::GroupingSets, groups, return_type.clone());

        // Verify children access
        assert_eq!(expr.get_children().len(), 3);
        assert_eq!(expr.get_child_at(0), &expr1);
        assert_eq!(expr.get_child_at(1), &expr2);
        assert_eq!(expr.get_child_at(2), &expr3);
    }

    #[test]
    fn test_display() {
        // Create test expressions
        let expr1 = create_test_expression(1);
        let expr2 = create_test_expression(2);
        let expr3 = create_test_expression(3);

        // Create groups
        let group1 = vec![expr1.clone(), expr2.clone()];
        let group2 = vec![expr3.clone()];

        let groups = vec![group1, group2];
        let return_type = Column::new("grouping_sets", TypeId::Vector);

        // Test GROUPING SETS
        let grouping_sets = GroupingSetsExpression::new(
            GroupingType::GroupingSets,
            groups.clone(),
            return_type.clone(),
        );
        assert!(grouping_sets.to_string().starts_with("GROUPING SETS"));

        // Test CUBE
        let cube =
            GroupingSetsExpression::new(GroupingType::Cube, groups.clone(), return_type.clone());
        assert!(cube.to_string().starts_with("CUBE"));

        // Test ROLLUP
        let rollup =
            GroupingSetsExpression::new(GroupingType::Rollup, groups.clone(), return_type.clone());
        assert!(rollup.to_string().starts_with("ROLLUP"));
    }

    #[test]
    fn test_validate() {
        // Create test expressions
        let expr1 = create_test_expression(1);
        let expr2 = create_test_expression(2);

        // Create groups
        let groups = vec![vec![expr1.clone(), expr2.clone()]];
        let return_type = Column::new("grouping_sets", TypeId::Vector);

        // Create GroupingSetsExpression
        let expr =
            GroupingSetsExpression::new(GroupingType::GroupingSets, groups, return_type.clone());

        // Create a schema
        let schema = Schema::new(vec![
            Column::new("const_1", TypeId::Integer),
            Column::new("const_2", TypeId::Integer),
        ]);

        // Validate should succeed
        assert!(expr.validate(&schema).is_ok());
    }

    #[test]
    fn test_clone_with_children() {
        // Create original expressions
        let expr1 = create_test_expression(1);
        let expr2 = create_test_expression(2);

        // Create replacement expressions
        let new_expr1 = create_test_expression(10);
        let new_expr2 = create_test_expression(20);

        // Create groups
        let groups = vec![vec![expr1.clone(), expr2.clone()]];
        let return_type = Column::new("grouping_sets", TypeId::Vector);

        // Create GroupingSetsExpression
        let expr =
            GroupingSetsExpression::new(GroupingType::GroupingSets, groups, return_type.clone());

        // Clone with new children
        let new_children = vec![new_expr1.clone(), new_expr2.clone()];
        let cloned = expr.clone_with_children(new_children);

        // Verify the cloned expression
        if let Expression::GroupingSets(gs_expr) = cloned.as_ref() {
            assert_eq!(gs_expr.get_grouping_type(), &GroupingType::GroupingSets);
            assert_eq!(gs_expr.get_groups().len(), 1);
            assert_eq!(gs_expr.get_groups()[0].len(), 2);

            // Check that the new expressions are used
            if let Expression::Constant(const_expr) = gs_expr.get_groups()[0][0].as_ref() {
                if let crate::types_db::value::Val::Integer(val) = const_expr.get_value().get_val()
                {
                    assert_eq!(*val, 10);
                } else {
                    panic!("Expected Integer value");
                }
            } else {
                panic!("Expected Constant expression");
            }
        } else {
            panic!("Expected GroupingSets expression");
        }
    }

    #[test]
    fn test_evaluate_errors() {
        // Create test expressions
        let expr1 = create_test_expression(1);
        let expr2 = create_test_expression(2);

        // Create groups
        let groups = vec![vec![expr1.clone(), expr2.clone()]];
        let return_type = Column::new("grouping_sets", TypeId::Vector);

        // Create GroupingSetsExpression
        let expr =
            GroupingSetsExpression::new(GroupingType::GroupingSets, groups, return_type.clone());

        // Create a schema and tuple for testing
        let schema = Schema::new(vec![
            Column::new("const_1", TypeId::Integer),
            Column::new("const_2", TypeId::Integer),
        ]);
        let tuple = Tuple::new(
            &[Value::new(1), Value::new(2)],
            schema.clone(),
            RID::new(1, 1),
        );

        // Evaluate should return an error
        assert!(expr.evaluate(&tuple, &schema).is_err());

        // Create a second tuple and schema for join evaluation
        let schema2 = Schema::new(vec![Column::new("const_3", TypeId::Integer)]);
        let tuple2 = Tuple::new(&[Value::new(3)], schema2.clone(), RID::new(2, 1));

        // Evaluate join should return an error
        assert!(expr
            .evaluate_join(&tuple, &schema, &tuple2, &schema2)
            .is_err());
    }
}
