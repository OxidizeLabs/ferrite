use std::fmt;

use crate::binder::bound_expression::{BoundExpression, ExpressionType};
use crate::binder::bound_order_by::BoundOrderBy;
use crate::binder::expressions::bound_constant::BoundConstant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowBoundary {
    Invalid = 0,
    UnboundedPreceding = 1,
    UnboundedFollowing = 2,
    CurrentRowRange = 3,
    CurrentRowRows = 4,
    ExprPrecedingRows = 5,
    ExprFollowingRows = 6,
    ExprPrecedingRange = 7,
    ExprFollowingRange = 8,
}

/// Represents a bound window function, e.g., `sum(x) OVER (PARTITION BY y ORDER BY z)`.
pub struct BoundWindow {
    /// Function name.
    pub func_name: String,
    /// Arguments of the function call.
    pub args: Vec<Box<dyn BoundExpression>>,
    /// PARTITION BY expressions.
    pub partition_by: Vec<Box<dyn BoundExpression>>,
    /// ORDER BY clauses.
    pub order_bys: Vec<Box<BoundOrderBy>>,
    /// Start offset expression.
    pub start_offset: Option<Box<dyn BoundExpression>>,
    /// End offset expression.
    pub end_offset: Option<Box<dyn BoundExpression>>,
    /// Window frame start boundary.
    pub start: WindowBoundary,
    /// Window frame end boundary.
    pub end: WindowBoundary,
}

impl BoundWindow {
    /// Creates a new BoundWindow.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        func_name: String,
        args: Vec<Box<dyn BoundExpression>>,
        partition_by: Vec<Box<dyn BoundExpression>>,
        order_bys: Vec<Box<BoundOrderBy>>,
        start_offset: Option<Box<dyn BoundExpression>>,
        end_offset: Option<Box<dyn BoundExpression>>,
        start: WindowBoundary,
        end: WindowBoundary,
    ) -> Self {
        Self {
            func_name,
            args,
            partition_by,
            order_bys,
            start_offset,
            end_offset,
            start,
            end,
        }
    }

    pub fn set_start(&mut self, start: WindowBoundary) {
        self.start = start;
    }

    pub fn set_end(&mut self, end: WindowBoundary) {
        self.end = end;
    }
}

impl BoundExpression for BoundWindow {
    fn expression_type(&self) -> ExpressionType {
        ExpressionType::Window
    }

    fn has_aggregation(&self) -> bool {
        false
    }

    fn has_window_function(&self) -> bool {
        true
    }
}

impl fmt::Display for BoundWindow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.func_name)?;
        for (i, arg) in self.args.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", arg)?;
        }
        write!(f, ") OVER (")?;

        if !self.partition_by.is_empty() {
            write!(f, "PARTITION BY ")?;
            for (i, expr) in self.partition_by.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", expr)?;
            }
        }

        if !self.order_bys.is_empty() {
            if !self.partition_by.is_empty() {
                write!(f, " ")?;
            }
            write!(f, "ORDER BY ")?;
            for (i, order_by) in self.order_bys.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{}", order_by)?;
            }
        }

        // TODO: Implement window frame clause formatting

        write!(f, ")")
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn bound_window() {
        let window = BoundWindow::new(
            "sum".to_string(),
            vec![Box::new(BoundConstant::new(1))],
            vec![Box::new(BoundConstant::new("y"))],
            vec![],
            None,
            None,
            WindowBoundary::UnboundedPreceding,
            WindowBoundary::CurrentRowRange,
        );

        assert_eq!(window.expression_type(), ExpressionType::Window);
        assert!(!window.has_aggregation());
        assert!(window.has_window_function());
        assert_eq!(window.to_string(), "sum(1) OVER (PARTITION BY \"y\")");
    }
}