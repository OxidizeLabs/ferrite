use std::fmt;
use std::sync::Arc;
use crate::catalogue::column::Column;
use crate::catalogue::schema::Schema;
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;

/// AbstractExpression is the base class of all the expressions in the system.
/// Expressions are modeled as trees, i.e., every expression may have a variable number of children.
pub trait AbstractExpression: fmt::Debug + Send + Sync {
    /// Evaluates the expression with the given tuple and schema.
    ///
    /// # Parameters
    /// - `tuple`: The tuple to evaluate.
    /// - `schema`: The schema of the tuple.
    ///
    /// # Returns
    /// The value obtained by evaluating the expression.
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Value;

    /// Evaluates the expression for a JOIN.
    ///
    /// # Parameters
    /// - `left_tuple`: The left tuple.
    /// - `left_schema`: The schema of the left tuple.
    /// - `right_tuple`: The right tuple.
    /// - `right_schema`: The schema of the right tuple.
    ///
    /// # Returns
    /// The value obtained by evaluating a JOIN on the left and right tuples.
    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Value;

    /// Returns the child at the specified index.
    ///
    /// # Parameters
    /// - `child_idx`: The index of the child to return.
    ///
    /// # Returns
    /// A reference to the child at the specified index.
    fn get_child_at(&self, child_idx: usize) -> Option<Arc<dyn AbstractExpression>>;

    /// Returns the children of this expression. The order may matter.
    ///
    /// # Returns
    /// A vector of references to the children of this expression.
    fn get_children(&self) -> Vec<Arc<dyn AbstractExpression>>;

    /// Returns the return type of this expression.
    ///
    /// # Returns
    /// The return type of this expression.
    fn get_return_type(&self) -> Column;

    /// Returns a string representation of the expression and its children.
    ///
    /// # Returns
    /// A string representation of the expression.
    fn to_string(&self) -> String;

    /// Clones the expression with new children.
    ///
    /// # Parameters
    /// - `children`: The new children of the expression.
    ///
    /// # Returns
    /// A new expression with the specified children.
    fn clone_with_children(&self, children: Vec<Arc<dyn AbstractExpression>>) -> Arc<dyn AbstractExpression>;
}

/// A macro to define the clone_with_children method for implementing structs.
#[macro_export]
macro_rules! impl_clone_with_children {
    ($struct_name:ident) => {
        fn clone_with_children(
            &self,
            children: Vec<Arc<dyn AbstractExpression>>,
        ) -> Arc<dyn AbstractExpression> {
            let mut expr = $struct_name {
                children,
                ..self.clone()
            };
            Arc::new(expr)
        }
    };
}

