use crate::types_db::type_id::TypeId;
use crate::types_db::types::{CmpBool, Type};
use crate::types_db::value::{Val, Value};

/// Implementation for PointType
#[derive(Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub struct PointType;

impl Default for PointType {
    fn default() -> Self {
        Self::new()
    }
}

impl PointType {
    pub fn new() -> Self {
        PointType
    }

    // Helper method to calculate Euclidean distance between points
    fn distance(x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
        ((x2 - x1).powi(2) + (y2 - y1).powi(2)).sqrt()
    }
}

impl Type for PointType {
    fn get_type_id(&self) -> TypeId {
        TypeId::Point
    }

    fn compare_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Point(x, y) => CmpBool::from(0.0 == *x && 0.0 == *y),
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_not_equals(&self, other: &Value) -> CmpBool {
        match self.compare_equals(other) {
            CmpBool::CmpTrue => CmpBool::CmpFalse,
            CmpBool::CmpFalse => CmpBool::CmpTrue,
            CmpBool::CmpNull => CmpBool::CmpNull,
        }
    }

    fn compare_less_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Point(x, y) => {
                // Compare based on distance from origin (0,0)
                let this_distance = Self::distance(0.0, 0.0, 0.0, 0.0);
                let other_distance = Self::distance(0.0, 0.0, *x, *y);
                CmpBool::from(this_distance < other_distance)
            },
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_less_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Point(x, y) => {
                // Compare based on distance from origin (0,0)
                let this_distance = Self::distance(0.0, 0.0, 0.0, 0.0);
                let other_distance = Self::distance(0.0, 0.0, *x, *y);
                CmpBool::from(this_distance <= other_distance)
            },
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Point(x, y) => {
                // Compare based on distance from origin (0,0)
                let this_distance = Self::distance(0.0, 0.0, 0.0, 0.0);
                let other_distance = Self::distance(0.0, 0.0, *x, *y);
                CmpBool::from(this_distance > other_distance)
            },
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn compare_greater_than_equals(&self, other: &Value) -> CmpBool {
        match other.get_val() {
            Val::Point(x, y) => {
                // Compare based on distance from origin (0,0)
                let this_distance = Self::distance(0.0, 0.0, 0.0, 0.0);
                let other_distance = Self::distance(0.0, 0.0, *x, *y);
                CmpBool::from(this_distance >= other_distance)
            },
            Val::Null => CmpBool::CmpNull,
            _ => CmpBool::CmpFalse,
        }
    }

    fn add(&self, other: &Value) -> Result<Value, String> {
        match other.get_val() {
            Val::Point(x, y) => Ok(Value::new(Val::Point(*x, *y))),
            Val::Null => Ok(Value::new(Val::Null)),
            _ => Err("Cannot add non-point value to Point".to_string()),
        }
    }

    fn subtract(&self, _other: &Value) -> Result<Value, String> {
        Err("Subtraction not directly supported for Point type".to_string())
    }

    fn multiply(&self, _other: &Value) -> Result<Value, String> {
        Err("Multiplication not supported for Point type".to_string())
    }

    fn divide(&self, _other: &Value) -> Result<Value, String> {
        Err("Division not supported for Point type".to_string())
    }

    fn modulo(&self, _other: &Value) -> Value {
        Value::new(Val::Null)
    }

    fn min(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Point(x, y) => {
                // Compare based on distance from origin
                let this_distance = Self::distance(0.0, 0.0, 0.0, 0.0);
                let other_distance = Self::distance(0.0, 0.0, *x, *y);
                if this_distance < other_distance {
                    Value::new(Val::Point(0.0, 0.0))
                } else {
                    Value::new(Val::Point(*x, *y))
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn max(&self, other: &Value) -> Value {
        match other.get_val() {
            Val::Point(x, y) => {
                // Compare based on distance from origin
                let this_distance = Self::distance(0.0, 0.0, 0.0, 0.0);
                let other_distance = Self::distance(0.0, 0.0, *x, *y);
                if this_distance > other_distance {
                    Value::new(Val::Point(0.0, 0.0))
                } else {
                    Value::new(Val::Point(*x, *y))
                }
            },
            _ => Value::new(Val::Null),
        }
    }

    fn to_string(&self, val: &Value) -> String {
        match val.get_val() {
            Val::Point(x, y) => format!("POINT({}, {})", x, y),
            Val::Null => "NULL".to_string(),
            _ => "INVALID".to_string(),
        }
    }
}

pub static POINT_TYPE_INSTANCE: PointType = PointType;
