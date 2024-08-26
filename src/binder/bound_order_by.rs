use std::fmt;

use crate::binder::bound_expression::BoundExpression;
use crate::binder::expressions::bound_constant::BoundConstant;

/// All types of order-bys in binder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderByType {
    Invalid = 0,
    Default = 1,
    Asc = 2,
    Desc = 3,
}

impl fmt::Display for OrderByType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OrderByType::Invalid => write!(f, "Invalid"),
            OrderByType::Default => write!(f, "Default"),
            OrderByType::Asc => write!(f, "Ascending"),
            OrderByType::Desc => write!(f, "Descending"),
        }
    }
}

/// BoundOrderBy is an item in the ORDER BY clause.
pub struct BoundOrderBy {
    /// The order by type.
    pub type_: OrderByType,
    /// The order by expression.
    pub expr: Box<dyn BoundExpression>,
}

impl BoundOrderBy {
    /// Creates a new BoundOrderBy.
    pub fn new(type_: OrderByType, expr: Box<dyn BoundExpression>) -> Self {
        Self { type_, expr }
    }
}

impl fmt::Display for BoundOrderBy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BoundOrderBy {{ type={}, expr={} }}", self.type_, self.expr)
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[test]
    fn bound_order_by() {
        let order_by = BoundOrderBy::new(
            OrderByType::Asc,
            Box::new(BoundConstant::new("column_name")),
        );

        assert_eq!(order_by.type_, OrderByType::Asc);
        assert_eq!(order_by.to_string(), "BoundOrderBy { type=Ascending, expr=\"column_name\" }");

        let desc_order_by = BoundOrderBy::new(
            OrderByType::Desc,
            Box::new(BoundConstant::new(42)),
        );

        assert_eq!(desc_order_by.type_, OrderByType::Desc);
        assert_eq!(desc_order_by.to_string(), "BoundOrderBy { type=Descending, expr=42 }");
    }

    #[test]
    fn order_by_type_display() {
        assert_eq!(format!("{}", OrderByType::Invalid), "Invalid");
        assert_eq!(format!("{}", OrderByType::Default), "Default");
        assert_eq!(format!("{}", OrderByType::Asc), "Ascending");
        assert_eq!(format!("{}", OrderByType::Desc), "Descending");
    }
}