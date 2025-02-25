use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::storage::table::tuple::Tuple;
use crate::types_db::value::Value;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use std::fmt;
use std::fmt::{Display, Formatter};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
pub enum DateTimeOperation {
    Add,
    Subtract,
    DateTrunc,
    DatePart,
    MakeDate,
    MakeTime,
    MakeTimestamp,
    MakeInterval,
}

#[derive(Clone, Debug, PartialEq)]
pub enum DateTimePart {
    Century,
    Decade,
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    DayOfWeek,
    DayOfYear,
    Epoch,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DateTimeExpression {
    operation: DateTimeOperation,
    args: Vec<Arc<Expression>>,
    part: Option<DateTimePart>,
    return_type: Column,
}

impl DateTimeExpression {
    pub fn new(
        operation: DateTimeOperation,
        args: Vec<Arc<Expression>>,
        part: Option<DateTimePart>,
        return_type: Column,
    ) -> Self {
        Self {
            operation,
            args,
            part,
            return_type,
        }
    }

    fn evaluate_date_trunc(&self, part: &DateTimePart, timestamp: DateTime<Utc>) -> Result<DateTime<Utc>, ExpressionError> {
        let naive = timestamp.naive_utc();
        let truncated = match part {
            DateTimePart::Year => {
                naive.date().and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?
            }
            DateTimePart::Month => {
                naive.date().and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?
            }
            // Implement other parts...
            _ => return Err(ExpressionError::InvalidOperation(
                format!("Unsupported date_trunc part: {:?}", part)
            )),
        };
        Ok(Utc.from_utc_datetime(&truncated))
    }

    fn evaluate_date_part(&self, part: &DateTimePart, timestamp: DateTime<Utc>) -> Result<i32, ExpressionError> {
        match part {
            DateTimePart::Year => Ok(timestamp.year()),
            DateTimePart::Month => Ok(timestamp.month() as i32),
            DateTimePart::Day => Ok(timestamp.day() as i32),
            // Implement other parts...
            _ => Err(ExpressionError::InvalidOperation(
                format!("Unsupported date_part: {:?}", part)
            )),
        }
    }
}

impl ExpressionOps for DateTimeExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match self.operation {
            DateTimeOperation::DateTrunc => {
                if let Some(part) = &self.part {
                    let timestamp = self.args[0].evaluate(tuple, schema)?;
                    // Parse timestamp string and perform truncation
                    let dt = DateTime::parse_from_rfc3339(&timestamp.to_string())
                        .map_err(|e| ExpressionError::InvalidOperation(e.to_string()))?
                        .with_timezone(&Utc);
                    let result = self.evaluate_date_trunc(part, dt)?;
                    Ok(Value::new(result.to_rfc3339()))
                } else {
                    Err(ExpressionError::InvalidOperation("Missing date part for date_trunc".to_string()))
                }
            }
            DateTimeOperation::DatePart => {
                if let Some(part) = &self.part {
                    let timestamp = self.args[0].evaluate(tuple, schema)?;
                    let dt = DateTime::parse_from_rfc3339(&timestamp.to_string())
                        .map_err(|e| ExpressionError::InvalidOperation(e.to_string()))?
                        .with_timezone(&Utc);
                    let result = self.evaluate_date_part(part, dt)?;
                    Ok(Value::new(result))
                } else {
                    Err(ExpressionError::InvalidOperation("Missing date part for date_part".to_string()))
                }
            }
            // Implement other operations...
            _ => Err(ExpressionError::InvalidOperation(
                format!("Unsupported datetime operation: {:?}", self.operation)
            )),
        }
    }

    fn evaluate_join(&self, left_tuple: &Tuple, left_schema: &Schema, right_tuple: &Tuple, right_schema: &Schema) -> Result<Value, ExpressionError> {
        todo!()
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        todo!()
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        todo!()
    }

    fn get_return_type(&self) -> &Column {
        todo!()
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        todo!()
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        todo!()
    }

    // Implement other required trait methods...
}

impl Display for DateTimeExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.operation {
            DateTimeOperation::DateTrunc => {
                write!(f, "date_trunc('{}', {})",
                       self.part.as_ref().unwrap(),
                       self.args[0]
                )
            }
            DateTimeOperation::DatePart => {
                write!(f, "date_part('{}', {})",
                       self.part.as_ref().unwrap(),
                       self.args[0]
                )
            }
            DateTimeOperation::Add => {
                write!(f, "{} + {}", self.args[0], self.args[1])
            }
            DateTimeOperation::Subtract => {
                write!(f, "{} - {}", self.args[0], self.args[1])
            }
            _ => write!(f, "{:?}({})",
                        self.operation,
                        self.args.iter()
                            .map(|arg| arg.to_string())
                            .collect::<Vec<_>>()
                            .join(", ")
            ),
        }
    }
} 