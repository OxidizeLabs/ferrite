use crate::catalog::column::Column;
use crate::catalog::schema::Schema;
use crate::common::exception::ExpressionError;
use crate::common::rid::RID;
use crate::sql::execution::expressions::abstract_expression::{Expression, ExpressionOps};
use crate::sql::execution::expressions::constant_value_expression::ConstantExpression;
use crate::storage::table::tuple::Tuple;
use crate::types_db::type_id::TypeId;
use crate::types_db::types::Type;
use crate::types_db::value::{Val, Value};
use chrono::{DateTime, Datelike, TimeZone, Timelike, Utc};
use regex::Regex;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::ops::Sub;
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
}

#[derive(Clone, Debug, PartialEq)]
pub enum DateTimeField {
    Century,
    Decade,
    Year,
    Quarter,
    Month,
    Week(Option<String>),
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Milliseconds,
    Microsecond,
    Microseconds,
    Nanosecond,
    Nanoseconds,
    DayOfWeek,
    Dow,
    DayOfYear,
    Doy,
    Epoch,
    Isodow,
    IsoWeek,
    Isoyear,
    Julian,
    Millenium,
    Millennium,
    Time,
    Date,
    Datetime,
    Timezone,
    TimezoneAbbr,
    TimezoneHour,
    TimezoneMinute,
    TimezoneRegion,
    Custom(String),
    NoDateTime,
}

#[derive(Clone, Debug, PartialEq)]
pub struct DateTimeExpression {
    operation: DateTimeOperation,
    args: Vec<Arc<Expression>>,
    part: Option<DateTimeField>,
    return_type: Column,
}

impl DateTimeExpression {
    pub fn new(
        operation: DateTimeOperation,
        args: Vec<Arc<Expression>>,
        part: Option<DateTimeField>,
        return_type: Column,
    ) -> Self {
        Self {
            operation,
            args,
            part,
            return_type,
        }
    }

    fn format_datetime_utc(dt: DateTime<Utc>) -> String {
        // Format with Z instead of +00:00 for UTC timezone
        dt.format("%Y-%m-%dT%H:%M:%SZ").to_string()
    }

    // Helper method to parse ISO 8601 duration strings
    fn parse_iso8601_duration(duration_str: &str) -> Result<chrono::Duration, String> {
        // Basic ISO 8601 duration parser for common formats
        // P1D = 1 day, P1M = 1 month, P1Y = 1 year, PT1H = 1 hour, PT1M = 1 minute, PT1S = 1 second

        let re = Regex::new(
            r"^P(?:(\d+)Y)?(?:(\d+)M)?(?:(\d+)D)?(?:T(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?)?$",
        )
        .map_err(|_| "Failed to compile regex".to_string())?;

        if let Some(captures) = re.captures(duration_str) {
            let years = captures
                .get(1)
                .and_then(|m| m.as_str().parse::<i64>().ok())
                .unwrap_or(0);
            let months = captures
                .get(2)
                .and_then(|m| m.as_str().parse::<i64>().ok())
                .unwrap_or(0);
            let days = captures
                .get(3)
                .and_then(|m| m.as_str().parse::<i64>().ok())
                .unwrap_or(0);
            let hours = captures
                .get(4)
                .and_then(|m| m.as_str().parse::<i64>().ok())
                .unwrap_or(0);
            let minutes = captures
                .get(5)
                .and_then(|m| m.as_str().parse::<i64>().ok())
                .unwrap_or(0);
            let seconds = captures
                .get(6)
                .and_then(|m| m.as_str().parse::<i64>().ok())
                .unwrap_or(0);

            // Convert to chrono::Duration
            // Note: This is a simplification - months and years are approximated
            let total_days = years * 365 + months * 30 + days;
            let total_seconds = hours * 3600 + minutes * 60 + seconds;

            Ok(chrono::Duration::days(total_days) + chrono::Duration::seconds(total_seconds))
        } else {
            Err(format!(
                "Invalid ISO 8601 duration format: {}",
                duration_str
            ))
        }
    }

    fn evaluate_date_trunc(
        &self,
        part: &DateTimeField,
        timestamp: DateTime<Utc>,
    ) -> Result<DateTime<Utc>, ExpressionError> {
        let naive = timestamp.naive_utc();
        let truncated = match part {
            DateTimeField::Year => naive
                .date()
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?,
            DateTimeField::Month => {
                let date = naive
                    .date()
                    .with_day(1)
                    .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?;
                date.and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?
            }
            DateTimeField::Quarter => {
                let month = (naive.month() - 1) / 3 * 3 + 1;
                naive
                    .date()
                    .with_month(month)
                    .unwrap()
                    .with_day(1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?
            }
            DateTimeField::Day => naive
                .date()
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?,
            DateTimeField::Hour => naive
                .date()
                .and_hms_opt(naive.hour(), 0, 0)
                .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?,
            DateTimeField::Minute => naive
                .date()
                .and_hms_opt(naive.hour(), naive.minute(), 0)
                .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?,
            DateTimeField::Century => {
                let year = (naive.year() / 100) * 100;
                naive
                    .date()
                    .with_year(year)
                    .unwrap()
                    .with_month(1)
                    .unwrap()
                    .with_day(1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?
            }
            DateTimeField::Decade => {
                let year = (naive.year() / 10) * 10;
                naive
                    .date()
                    .with_year(year)
                    .unwrap()
                    .with_month(1)
                    .unwrap()
                    .with_day(1)
                    .unwrap()
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?
            }
            DateTimeField::Week(Some(weekday)) => {
                let weekday = naive.weekday().num_days_from_monday();
                naive
                    .date()
                    .sub(chrono::Duration::days(weekday as i64))
                    .and_hms_opt(0, 0, 0)
                    .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?
            }
            DateTimeField::Second => naive
                .date()
                .and_hms_opt(naive.hour(), naive.minute(), naive.second())
                .ok_or_else(|| ExpressionError::InvalidOperation("Invalid date".to_string()))?,
            _ => {
                return Err(ExpressionError::InvalidOperation(format!(
                    "Unsupported date_trunc part: {:?}",
                    part
                )))
            }
        };
        Ok(Utc.from_utc_datetime(&truncated))
    }

    fn evaluate_date_part(
        &self,
        part: &DateTimeField,
        timestamp: DateTime<Utc>,
    ) -> Result<i32, ExpressionError> {
        match part {
            DateTimeField::Year => Ok(timestamp.year()),
            DateTimeField::Month => Ok(timestamp.month() as i32),
            DateTimeField::Day => Ok(timestamp.day() as i32),
            DateTimeField::DayOfWeek | DateTimeField::Dow => {
                Ok(timestamp.weekday().num_days_from_monday() as i32)
            }
            DateTimeField::DayOfYear | DateTimeField::Doy => Ok(timestamp.ordinal() as i32),
            DateTimeField::Hour => Ok(timestamp.hour() as i32),
            DateTimeField::Minute => Ok(timestamp.minute() as i32),
            DateTimeField::Second => Ok(timestamp.second() as i32),
            DateTimeField::Quarter => Ok(((timestamp.month() - 1) / 3 + 1) as i32),
            DateTimeField::Week(Some(weekday)) => Ok(timestamp.iso_week().week() as i32),
            DateTimeField::IsoWeek => Ok(timestamp.iso_week().week() as i32),
            DateTimeField::Isoyear => Ok(timestamp.iso_week().year()),
            DateTimeField::Isodow => Ok(timestamp.weekday().number_from_monday() as i32),
            DateTimeField::Epoch => Ok(timestamp.timestamp() as i32),
            DateTimeField::Millisecond | DateTimeField::Milliseconds => {
                Ok((timestamp.timestamp_subsec_millis() % 1000) as i32)
            }
            DateTimeField::Microsecond | DateTimeField::Microseconds => {
                Ok((timestamp.timestamp_subsec_micros() % 1_000_000) as i32)
            }
            DateTimeField::Nanosecond | DateTimeField::Nanoseconds => {
                Ok((timestamp.timestamp_subsec_nanos() % 1_000_000_000) as i32)
            }
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported date_part: {:?}",
                part
            ))),
        }
    }

    fn evaluate_make_date(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        if self.args.len() != 3 {
            return Err(ExpressionError::InvalidOperation(
                "make_date requires year, month, and day arguments".to_string(),
            ));
        }

        let year = self.args[0].evaluate(tuple, schema)?.as_integer().unwrap();
        let month = self.args[1].evaluate(tuple, schema)?.as_integer().unwrap();
        let day = self.args[2].evaluate(tuple, schema)?.as_integer().unwrap();

        let date = Utc
            .with_ymd_and_hms(year, month as u32, day as u32, 0, 0, 0)
            .earliest()
            .ok_or_else(|| {
                ExpressionError::InvalidOperation("Invalid date components".to_string())
            })?;

        Ok(Value::new(Self::format_datetime_utc(date)))
    }

    fn evaluate_make_time(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        if self.args.len() != 3 {
            return Err(ExpressionError::InvalidOperation(
                "make_time requires hour, minute, and second arguments".to_string(),
            ));
        }

        let hour = self.args[0].evaluate(tuple, schema)?.as_integer().unwrap();
        let min = self.args[1].evaluate(tuple, schema)?.as_integer().unwrap();
        let sec = self.args[2].evaluate(tuple, schema)?.as_integer().unwrap();

        let time = Utc
            .with_ymd_and_hms(0, 0, 0, hour as u32, min as u32, sec as u32)
            .earliest()
            .ok_or_else(|| {
                ExpressionError::InvalidOperation("Invalid time components".to_string())
            })?;

        Ok(Value::new(Self::format_datetime_utc(time)))
    }

    fn evaluate_make_timestamp(
        &self,
        tuple: &Tuple,
        schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        if self.args.len() != 6 {
            return Err(ExpressionError::InvalidOperation(
                "make_timestamp requires year, month, day, hour, minute, and second arguments"
                    .to_string(),
            ));
        }

        let year = self.args[0].evaluate(tuple, schema)?.as_integer().unwrap();
        let month = self.args[1].evaluate(tuple, schema)?.as_integer().unwrap();
        let day = self.args[2].evaluate(tuple, schema)?.as_integer().unwrap();
        let hour = self.args[3].evaluate(tuple, schema)?.as_integer().unwrap();
        let min = self.args[4].evaluate(tuple, schema)?.as_integer().unwrap();
        let sec = self.args[5].evaluate(tuple, schema)?.as_integer().unwrap();

        let timestamp = Utc
            .with_ymd_and_hms(
                year,
                month as u32,
                day as u32,
                hour as u32,
                min as u32,
                sec as u32,
            )
            .earliest()
            .ok_or_else(|| {
                ExpressionError::InvalidOperation("Invalid timestamp components".to_string())
            })?;

        Ok(Value::new(Self::format_datetime_utc(timestamp)))
    }
}

impl ExpressionOps for DateTimeExpression {
    fn evaluate(&self, tuple: &Tuple, schema: &Schema) -> Result<Value, ExpressionError> {
        match self.operation {
            DateTimeOperation::DateTrunc => {
                if let Some(part) = &self.part {
                    if self.args.len() != 1 {
                        return Err(ExpressionError::InvalidOperation(
                            "date_trunc requires exactly one argument".to_string(),
                        ));
                    }

                    let timestamp_val = self.args[0].evaluate(tuple, schema)?;
                    let timestamp = match timestamp_val.get_val() {
                        Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                            DateTime::parse_from_rfc3339(ts_str)
                                .map_err(|e| {
                                    ExpressionError::InvalidOperation(format!(
                                        "Invalid timestamp format: {}",
                                        e
                                    ))
                                })?
                                .with_timezone(&Utc)
                        }
                        Val::Timestamp(ts) => {
                            Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                ExpressionError::InvalidOperation(
                                    "Invalid timestamp value".to_string(),
                                )
                            })?
                        }
                        _ => {
                            return Err(ExpressionError::InvalidOperation(
                                "Expected timestamp value".to_string(),
                            ))
                        }
                    };
                    let result = self.evaluate_date_trunc(part, timestamp)?;
                    Ok(Value::new(Self::format_datetime_utc(result)))
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "Missing date part for date_trunc".to_string(),
                    ))
                }
            }
            DateTimeOperation::DatePart => {
                if let Some(part) = &self.part {
                    if self.args.len() != 1 {
                        return Err(ExpressionError::InvalidOperation(
                            "date_part requires exactly one argument".to_string(),
                        ));
                    }

                    let timestamp_val = self.args[0].evaluate(tuple, schema)?;
                    let timestamp = match timestamp_val.get_val() {
                        Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                            DateTime::parse_from_rfc3339(ts_str)
                                .map_err(|e| {
                                    ExpressionError::InvalidOperation(format!(
                                        "Invalid timestamp format: {}",
                                        e
                                    ))
                                })?
                                .with_timezone(&Utc)
                        }
                        Val::Timestamp(ts) => {
                            Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                ExpressionError::InvalidOperation(
                                    "Invalid timestamp value".to_string(),
                                )
                            })?
                        }
                        _ => {
                            return Err(ExpressionError::InvalidOperation(
                                "Expected timestamp value".to_string(),
                            ))
                        }
                    };
                    let result = self.evaluate_date_part(part, timestamp)?;
                    Ok(Value::new(result))
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "Missing date part for date_part".to_string(),
                    ))
                }
            }
            DateTimeOperation::MakeDate => self.evaluate_make_date(tuple, schema),
            DateTimeOperation::MakeTime => self.evaluate_make_time(tuple, schema),
            DateTimeOperation::MakeTimestamp => self.evaluate_make_timestamp(tuple, schema),
            DateTimeOperation::Add | DateTimeOperation::Subtract => {
                if self.args.len() != 2 {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "{:?} operation requires exactly two arguments",
                        self.operation
                    )));
                }

                let left = self.args[0].evaluate(tuple, schema)?;
                let right = self.args[1].evaluate(tuple, schema)?;

                match (left.get_val(), right.get_val()) {
                    // Timestamp + Interval (for addition)
                    (
                        Val::Timestamp(_) | Val::VarLen(_) | Val::ConstLen(_),
                        Val::VarLen(interval_str) | Val::ConstLen(interval_str),
                    ) if self.operation == DateTimeOperation::Add => {
                        let timestamp = match left.get_val() {
                            Val::Timestamp(ts) => {
                                Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                    ExpressionError::InvalidOperation(
                                        "Invalid timestamp value".to_string(),
                                    )
                                })?
                            }
                            Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                                DateTime::parse_from_rfc3339(ts_str)
                                    .map_err(|e| {
                                        ExpressionError::InvalidOperation(format!(
                                            "Invalid timestamp format: {}",
                                            e
                                        ))
                                    })?
                                    .with_timezone(&Utc)
                            }
                            _ => unreachable!(),
                        };

                        // Parse ISO 8601 duration string
                        let duration = Self::parse_iso8601_duration(interval_str).map_err(|e| {
                            ExpressionError::InvalidOperation(format!(
                                "Invalid interval format: {}",
                                e
                            ))
                        })?;

                        // Add duration to timestamp
                        let result = timestamp + duration;
                        Ok(Value::new(Self::format_datetime_utc(result)))
                    }
                    // Interval + Timestamp (for addition)
                    (
                        Val::VarLen(interval_str) | Val::ConstLen(interval_str),
                        Val::Timestamp(_) | Val::VarLen(_) | Val::ConstLen(_),
                    ) if self.operation == DateTimeOperation::Add => {
                        let timestamp = match right.get_val() {
                            Val::Timestamp(ts) => {
                                Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                    ExpressionError::InvalidOperation(
                                        "Invalid timestamp value".to_string(),
                                    )
                                })?
                            }
                            Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                                DateTime::parse_from_rfc3339(ts_str)
                                    .map_err(|e| {
                                        ExpressionError::InvalidOperation(format!(
                                            "Invalid timestamp format: {}",
                                            e
                                        ))
                                    })?
                                    .with_timezone(&Utc)
                            }
                            _ => unreachable!(),
                        };

                        // Parse ISO 8601 duration string
                        let duration = Self::parse_iso8601_duration(interval_str).map_err(|e| {
                            ExpressionError::InvalidOperation(format!(
                                "Invalid interval format: {}",
                                e
                            ))
                        })?;

                        // Add duration to timestamp
                        let result = timestamp + duration;
                        Ok(Value::new(Self::format_datetime_utc(result)))
                    }
                    // Timestamp - Interval (for subtraction)
                    (
                        Val::Timestamp(_) | Val::VarLen(_) | Val::ConstLen(_),
                        Val::VarLen(interval_str) | Val::ConstLen(interval_str),
                    ) if self.operation == DateTimeOperation::Subtract => {
                        let timestamp = match left.get_val() {
                            Val::Timestamp(ts) => {
                                Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                    ExpressionError::InvalidOperation(
                                        "Invalid timestamp value".to_string(),
                                    )
                                })?
                            }
                            Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                                DateTime::parse_from_rfc3339(ts_str)
                                    .map_err(|e| {
                                        ExpressionError::InvalidOperation(format!(
                                            "Invalid timestamp format: {}",
                                            e
                                        ))
                                    })?
                                    .with_timezone(&Utc)
                            }
                            _ => unreachable!(),
                        };

                        // Parse ISO 8601 duration string
                        let duration = Self::parse_iso8601_duration(interval_str).map_err(|e| {
                            ExpressionError::InvalidOperation(format!(
                                "Invalid interval format: {}",
                                e
                            ))
                        })?;

                        // Subtract duration from timestamp
                        let result = timestamp - duration;
                        Ok(Value::new(Self::format_datetime_utc(result)))
                    }
                    // Timestamp - Timestamp (only for subtraction)
                    (
                        Val::Timestamp(_) | Val::VarLen(_) | Val::ConstLen(_),
                        Val::Timestamp(_) | Val::VarLen(_) | Val::ConstLen(_),
                    ) if self.operation == DateTimeOperation::Subtract => {
                        let timestamp1 = match left.get_val() {
                            Val::Timestamp(ts) => {
                                Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                    ExpressionError::InvalidOperation(
                                        "Invalid timestamp value".to_string(),
                                    )
                                })?
                            }
                            Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                                DateTime::parse_from_rfc3339(ts_str)
                                    .map_err(|e| {
                                        ExpressionError::InvalidOperation(format!(
                                            "Invalid timestamp format: {}",
                                            e
                                        ))
                                    })?
                                    .with_timezone(&Utc)
                            }
                            _ => unreachable!(),
                        };
                        let timestamp2 = match right.get_val() {
                            Val::Timestamp(ts) => {
                                Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                    ExpressionError::InvalidOperation(
                                        "Invalid timestamp value".to_string(),
                                    )
                                })?
                            }
                            Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                                DateTime::parse_from_rfc3339(ts_str)
                                    .map_err(|e| {
                                        ExpressionError::InvalidOperation(format!(
                                            "Invalid timestamp format: {}",
                                            e
                                        ))
                                    })?
                                    .with_timezone(&Utc)
                            }
                            _ => unreachable!(),
                        };

                        let duration = timestamp1.sub(timestamp2);
                        let duration_str = format!("{:?}", duration);
                        Ok(Value::new(duration_str))
                    }
                    _ => Err(ExpressionError::InvalidOperation(format!(
                        "Invalid operands for {:?} operation: {:?} and {:?}",
                        self.operation,
                        left.get_val(),
                        right.get_val()
                    ))),
                }
            }
            _ => Err(ExpressionError::InvalidOperation(format!(
                "Unsupported datetime operation: {:?}",
                self.operation
            ))),
        }
    }

    fn evaluate_join(
        &self,
        left_tuple: &Tuple,
        left_schema: &Schema,
        right_tuple: &Tuple,
        right_schema: &Schema,
    ) -> Result<Value, ExpressionError> {
        match self.operation {
            DateTimeOperation::DateTrunc => {
                if let Some(part) = &self.part {
                    if self.args.len() != 1 {
                        return Err(ExpressionError::InvalidOperation(
                            "date_trunc requires exactly one argument".to_string(),
                        ));
                    }

                    // Check if the argument is a ColumnRefExpression and use the appropriate tuple based on tuple_index
                    let timestamp_val =
                        if let Expression::ColumnRef(col_ref) = self.args[0].as_ref() {
                            if col_ref.get_tuple_index() == 0 {
                                // Use left tuple
                                self.args[0].evaluate(left_tuple, left_schema)?
                            } else {
                                // Use right tuple
                                self.args[0].evaluate(right_tuple, right_schema)?
                            }
                        } else {
                            // For non-column expressions, try both tuples
                            match self.args[0].evaluate(left_tuple, left_schema) {
                                Ok(val) => val,
                                Err(_) => self.args[0].evaluate(right_tuple, right_schema)?,
                            }
                        };

                    let timestamp = match timestamp_val.get_val() {
                        Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                            DateTime::parse_from_rfc3339(ts_str)
                                .map_err(|e| {
                                    ExpressionError::InvalidOperation(format!(
                                        "Invalid timestamp format: {}",
                                        e
                                    ))
                                })?
                                .with_timezone(&Utc)
                        }
                        Val::Timestamp(ts) => {
                            Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                ExpressionError::InvalidOperation(
                                    "Invalid timestamp value".to_string(),
                                )
                            })?
                        }
                        _ => {
                            return Err(ExpressionError::InvalidOperation(
                                "Expected timestamp value".to_string(),
                            ))
                        }
                    };
                    let result = self.evaluate_date_trunc(part, timestamp)?;
                    Ok(Value::new(Self::format_datetime_utc(result)))
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "Missing date part for date_trunc".to_string(),
                    ))
                }
            }
            DateTimeOperation::DatePart => {
                if let Some(part) = &self.part {
                    if self.args.len() != 1 {
                        return Err(ExpressionError::InvalidOperation(
                            "date_part requires exactly one argument".to_string(),
                        ));
                    }

                    // Try to evaluate the argument using both schemas
                    let timestamp_val = match self.args[0].evaluate(left_tuple, left_schema) {
                        Ok(val) => val,
                        Err(_) => self.args[0].evaluate(right_tuple, right_schema)?,
                    };

                    let timestamp = match timestamp_val.get_val() {
                        Val::VarLen(ts_str) | Val::ConstLen(ts_str) => {
                            DateTime::parse_from_rfc3339(ts_str)
                                .map_err(|e| {
                                    ExpressionError::InvalidOperation(format!(
                                        "Invalid timestamp format: {}",
                                        e
                                    ))
                                })?
                                .with_timezone(&Utc)
                        }
                        Val::Timestamp(ts) => {
                            Utc.timestamp_opt(*ts as i64, 0).single().ok_or_else(|| {
                                ExpressionError::InvalidOperation(
                                    "Invalid timestamp value".to_string(),
                                )
                            })?
                        }
                        _ => {
                            return Err(ExpressionError::InvalidOperation(
                                "Expected timestamp value".to_string(),
                            ))
                        }
                    };
                    let result = self.evaluate_date_part(part, timestamp)?;
                    Ok(Value::new(result))
                } else {
                    Err(ExpressionError::InvalidOperation(
                        "Missing date part for date_part".to_string(),
                    ))
                }
            }
            DateTimeOperation::MakeDate
            | DateTimeOperation::MakeTime
            | DateTimeOperation::MakeTimestamp
            | DateTimeOperation::Add
            | DateTimeOperation::Subtract => {
                if self.args.len() != 2 {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "{:?} operation requires exactly two arguments",
                        self.operation
                    )));
                }

                // Try to evaluate each argument using both schemas
                let left = match self.args[0].evaluate(left_tuple, left_schema) {
                    Ok(val) => val,
                    Err(_) => self.args[0].evaluate(right_tuple, right_schema)?,
                };

                let right = match self.args[1].evaluate(left_tuple, left_schema) {
                    Ok(val) => val,
                    Err(_) => self.args[1].evaluate(right_tuple, right_schema)?,
                };

                // Create a dummy tuple with the evaluated arguments
                let mut dummy_tuple = Tuple::new(&[], Schema::new(vec![]), RID::new(0, 0));
                let values = dummy_tuple.get_values_mut();
                values.push(left);
                values.push(right);

                // Use the regular evaluate method with the dummy tuple
                self.evaluate(&dummy_tuple, &Schema::new(vec![]))
            }
        }
    }

    fn get_child_at(&self, child_idx: usize) -> &Arc<Expression> {
        if child_idx < self.args.len() {
            &self.args[child_idx]
        } else {
            panic!("Child index out of bounds")
        }
    }

    fn get_children(&self) -> &Vec<Arc<Expression>> {
        &self.args
    }

    fn get_return_type(&self) -> &Column {
        &self.return_type
    }

    fn clone_with_children(&self, children: Vec<Arc<Expression>>) -> Arc<Expression> {
        Arc::new(Expression::DateTime(DateTimeExpression {
            operation: self.operation.clone(),
            args: children,
            part: self.part.clone(),
            return_type: self.return_type.clone(),
        }))
    }

    fn validate(&self, schema: &Schema) -> Result<(), ExpressionError> {
        // Validate all child expressions
        for arg in &self.args {
            arg.validate(schema)?;
        }

        // Validate operation-specific requirements
        match self.operation {
            DateTimeOperation::DateTrunc | DateTimeOperation::DatePart => {
                if self.args.len() != 1 {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "{:?} requires exactly one argument",
                        self.operation
                    )));
                }
                if self.part.is_none() {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "Missing date part for {:?}",
                        self.operation
                    )));
                }

                // Check that the argument is a timestamp or can be converted to one
                let arg_type = self.args[0].get_return_type();
                if arg_type.get_type() != TypeId::Timestamp
                    && arg_type.get_type() != TypeId::VarChar
                {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "Expected timestamp or string argument for {:?}, got {:?}",
                        self.operation,
                        arg_type.get_type()
                    )));
                }
            }
            DateTimeOperation::MakeDate => {
                if self.args.len() != 3 {
                    return Err(ExpressionError::InvalidOperation(
                        "make_date requires year, month, and day arguments".to_string(),
                    ));
                }
                // Ensure all arguments are integers
                for (i, arg) in self.args.iter().enumerate() {
                    let arg_type = arg.get_return_type();
                    if arg_type.get_type() != TypeId::Integer {
                        return Err(ExpressionError::InvalidOperation(format!(
                            "Expected integer for argument {} of make_date, got {:?}",
                            i + 1,
                            arg_type.get_type()
                        )));
                    }
                }
            }
            DateTimeOperation::MakeTime => {
                if self.args.len() != 3 {
                    return Err(ExpressionError::InvalidOperation(
                        "make_time requires hour, minute, and second arguments".to_string(),
                    ));
                }
                // Ensure all arguments are integers
                for (i, arg) in self.args.iter().enumerate() {
                    let arg_type = arg.get_return_type();
                    if arg_type.get_type() != TypeId::Integer {
                        return Err(ExpressionError::InvalidOperation(format!(
                            "Expected integer for argument {} of make_time, got {:?}",
                            i + 1,
                            arg_type.get_type()
                        )));
                    }
                }
            }
            DateTimeOperation::MakeTimestamp => {
                if self.args.len() != 6 {
                    return Err(ExpressionError::InvalidOperation(
                        "make_timestamp requires year, month, day, hour, minute, and second arguments".to_string()
                    ));
                }
                // Ensure all arguments are integers
                for (i, arg) in self.args.iter().enumerate() {
                    let arg_type = arg.get_return_type();
                    if arg_type.get_type() != TypeId::Integer {
                        return Err(ExpressionError::InvalidOperation(format!(
                            "Expected integer for argument {} of make_timestamp, got {:?}",
                            i + 1,
                            arg_type.get_type()
                        )));
                    }
                }
            }
            DateTimeOperation::Add | DateTimeOperation::Subtract => {
                if self.args.len() != 2 {
                    return Err(ExpressionError::InvalidOperation(format!(
                        "{:?} operation requires exactly two arguments",
                        self.operation
                    )));
                }

                let left_type = self.args[0].get_return_type();
                let right_type = self.args[1].get_return_type();

                // For addition: timestamp + interval or interval + timestamp
                // For subtraction: timestamp - interval or timestamp - timestamp
                match self.operation {
                    DateTimeOperation::Add => {
                        let valid = (left_type.get_type() == TypeId::Timestamp
                            && right_type.get_type() == TypeId::VarChar)
                            || (left_type.get_type() == TypeId::VarChar
                                && right_type.get_type() == TypeId::Timestamp);

                        if !valid {
                            return Err(ExpressionError::InvalidOperation(
                                "Addition requires timestamp and interval".to_string(),
                            ));
                        }
                    }
                    DateTimeOperation::Subtract => {
                        let valid = (left_type.get_type() == TypeId::Timestamp
                            && right_type.get_type() == TypeId::VarChar)
                            || (left_type.get_type() == TypeId::Timestamp
                                && right_type.get_type() == TypeId::Timestamp);

                        if !valid {
                            return Err(ExpressionError::InvalidOperation(
                                "Subtraction requires timestamp - interval or timestamp - timestamp".to_string()
                            ));
                        }
                    }
                    _ => unreachable!(),
                }
            }
        }

        Ok(())
    }
}

impl Display for DateTimeExpression {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.operation {
            DateTimeOperation::DateTrunc => {
                write!(
                    f,
                    "date_trunc('{}', {})",
                    self.part.as_ref().unwrap(),
                    self.args[0]
                )
            }
            DateTimeOperation::DatePart => {
                write!(
                    f,
                    "date_part('{}', {})",
                    self.part.as_ref().unwrap(),
                    self.args[0]
                )
            }
            DateTimeOperation::MakeDate => {
                write!(
                    f,
                    "make_date({}, {}, {})",
                    self.args[0], self.args[1], self.args[2]
                )
            }
            DateTimeOperation::MakeTime => {
                write!(
                    f,
                    "make_time({}, {}, {})",
                    self.args[0], self.args[1], self.args[2]
                )
            }
            DateTimeOperation::MakeTimestamp => {
                write!(
                    f,
                    "make_timestamp({}, {}, {}, {}, {}, {})",
                    self.args[0],
                    self.args[1],
                    self.args[2],
                    self.args[3],
                    self.args[4],
                    self.args[5]
                )
            }
            DateTimeOperation::Add => {
                write!(f, "{} + {}", self.args[0], self.args[1])
            }
            DateTimeOperation::Subtract => {
                write!(f, "{} - {}", self.args[0], self.args[1])
            }
            _ => write!(
                f,
                "{:?}({})",
                self.operation,
                self.args
                    .iter()
                    .map(|arg| arg.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        }
    }
}

impl Display for DateTimeField {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            DateTimeField::Week(Some(weekday)) => write!(f, "week({})", weekday),
            DateTimeField::Custom(ident) => write!(f, "{}", ident),
            DateTimeField::DayOfWeek => write!(f, "dow"),
            DateTimeField::DayOfYear => write!(f, "doy"),
            DateTimeField::Milliseconds => write!(f, "milliseconds"),
            DateTimeField::Microseconds => write!(f, "microseconds"),
            DateTimeField::Nanoseconds => write!(f, "nanoseconds"),
            _ => write!(f, "{:?}", self),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::Column;
    use crate::catalog::schema::Schema;
    use crate::sql::execution::expressions::column_value_expression::ColumnRefExpression;
    use crate::storage::table::tuple::Tuple;
    use crate::types_db::type_id::TypeId;
    use crate::types_db::value::{Val, Value};

    fn create_test_schema() -> Schema {
        Schema::new(vec![
            Column::new("timestamp_col", TypeId::Timestamp),
            Column::new("int_col", TypeId::Integer),
        ])
    }

    fn create_test_tuple() -> Tuple {
        let mut tuple = Tuple::new(&[], Schema::new(vec![]), RID::new(0, 0));
        let values = tuple.get_values_mut();
        values.push(Value::new("2024-01-01T12:30:45Z"));
        values.push(Value::new(42));
        tuple
    }

    #[test]
    fn test_date_trunc() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        // Test year truncation
        let year_expr = DateTimeExpression::new(
            DateTimeOperation::DateTrunc,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Year),
            Column::new("result", TypeId::VarChar),
        );
        let result = year_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-01-01T00:00:00Z".to_string())
        );

        // Test month truncation
        let month_expr = DateTimeExpression::new(
            DateTimeOperation::DateTrunc,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Month),
            Column::new("result", TypeId::VarChar),
        );
        let result = month_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-01-01T00:00:00Z".to_string())
        );

        // Test day truncation
        let day_expr = DateTimeExpression::new(
            DateTimeOperation::DateTrunc,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Day),
            Column::new("result", TypeId::VarChar),
        );
        let result = day_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-01-01T00:00:00Z".to_string())
        );

        // Test hour truncation
        let hour_expr = DateTimeExpression::new(
            DateTimeOperation::DateTrunc,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Hour),
            Column::new("result", TypeId::VarChar),
        );
        let result = hour_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-01-01T12:00:00Z".to_string())
        );
    }

    #[test]
    fn test_date_part() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        // Test year extraction
        let year_expr = DateTimeExpression::new(
            DateTimeOperation::DatePart,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Year),
            Column::new("result", TypeId::Integer),
        );
        let result = year_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Integer(2024));

        // Test month extraction
        let month_expr = DateTimeExpression::new(
            DateTimeOperation::DatePart,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Month),
            Column::new("result", TypeId::Integer),
        );
        let result = month_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Integer(1));

        // Test hour extraction
        let hour_expr = DateTimeExpression::new(
            DateTimeOperation::DatePart,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Hour),
            Column::new("result", TypeId::Integer),
        );
        let result = hour_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(result.get_val(), &Val::Integer(12));
    }

    #[test]
    fn test_make_date() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        let make_date_expr = DateTimeExpression::new(
            DateTimeOperation::MakeDate,
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2024),
                    Column::new("year", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(1),
                    Column::new("month", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(15),
                    Column::new("day", TypeId::Integer),
                    vec![],
                ))),
            ],
            None,
            Column::new("result", TypeId::VarChar),
        );

        let result = make_date_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-01-15T00:00:00Z".to_string())
        );
    }

    #[test]
    fn test_make_timestamp() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        let make_timestamp_expr = DateTimeExpression::new(
            DateTimeOperation::MakeTimestamp,
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2024),
                    Column::new("year", TypeId::Integer),
                    vec![],
                ))), // year
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(1),
                    Column::new("month", TypeId::Integer),
                    vec![],
                ))), // month
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(15),
                    Column::new("day", TypeId::Integer),
                    vec![],
                ))), // day
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(14),
                    Column::new("hour", TypeId::Integer),
                    vec![],
                ))), // hour
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(30),
                    Column::new("minute", TypeId::Integer),
                    vec![],
                ))), // minute
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(45),
                    Column::new("second", TypeId::Integer),
                    vec![],
                ))), // second
            ],
            None,
            Column::new("result", TypeId::VarChar),
        );

        let result = make_timestamp_expr.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-01-15T14:30:45Z".to_string())
        );
    }

    #[test]
    fn test_invalid_operations() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        // Test date_trunc with invalid part
        let invalid_trunc = DateTimeExpression::new(
            DateTimeOperation::DateTrunc,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Custom("invalid".to_string())),
            Column::new("result", TypeId::VarChar),
        );
        assert!(invalid_trunc.evaluate(&tuple, &schema).is_err());

        // Test date_part with invalid part
        let invalid_part = DateTimeExpression::new(
            DateTimeOperation::DatePart,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Custom("invalid".to_string())),
            Column::new("result", TypeId::Integer),
        );
        assert!(invalid_part.evaluate(&tuple, &schema).is_err());

        // Test make_date with wrong number of arguments
        let invalid_make_date = DateTimeExpression::new(
            DateTimeOperation::MakeDate,
            vec![Arc::new(Expression::Constant(ConstantExpression::new(
                Value::new(2024),
                Column::new("year", TypeId::Integer),
                vec![],
            )))],
            None,
            Column::new("result", TypeId::VarChar),
        );
        assert!(invalid_make_date.evaluate(&tuple, &schema).is_err());
    }

    #[test]
    fn test_datetime_field_display() {
        assert_eq!(DateTimeField::Year.to_string(), "Year");
        assert_eq!(DateTimeField::Month.to_string(), "Month");
        assert_eq!(DateTimeField::DayOfWeek.to_string(), "dow");
        assert_eq!(
            DateTimeField::Week(Some("monday".to_string())).to_string(),
            "week(monday)"
        );
        assert_eq!(
            DateTimeField::Custom("test".to_string()).to_string(),
            "test"
        );
    }

    #[test]
    fn test_datetime_expression_display() {
        let schema = create_test_schema();

        let date_trunc_expr = DateTimeExpression::new(
            DateTimeOperation::DateTrunc,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("timestamp", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Year),
            Column::new("result", TypeId::VarChar),
        );
        assert_eq!(date_trunc_expr.to_string(), "date_trunc('Year', timestamp)");

        let make_date_expr = DateTimeExpression::new(
            DateTimeOperation::MakeDate,
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(2024),
                    Column::new("year", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(1),
                    Column::new("month", TypeId::Integer),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new(15),
                    Column::new("day", TypeId::Integer),
                    vec![],
                ))),
            ],
            None,
            Column::new("result", TypeId::VarChar),
        );
        assert_eq!(make_date_expr.to_string(), "make_date(2024, 1, 15)");
    }

    #[test]
    fn test_datetime_join_evaluation() {
        let left_schema = Schema::new(vec![Column::new("left_ts", TypeId::Timestamp)]);
        let right_schema = Schema::new(vec![Column::new("right_ts", TypeId::Timestamp)]);

        let mut left_tuple = Tuple::new(&[], Schema::new(vec![]), RID::new(0, 0));
        left_tuple
            .get_values_mut()
            .push(Value::new("2024-01-01T12:30:45Z"));

        let mut right_tuple = Tuple::new(&[], Schema::new(vec![]), RID::new(0, 0));
        right_tuple
            .get_values_mut()
            .push(Value::new("2024-02-15T14:20:30Z"));

        // Test date_trunc with left tuple column
        let left_trunc = DateTimeExpression::new(
            DateTimeOperation::DateTrunc,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                0,
                0,
                Column::new("left_ts", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Month),
            Column::new("result", TypeId::VarChar),
        );

        let result = left_trunc
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();
        println!("Left result: {:?}", result.get_val());
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-01-01T00:00:00Z".to_string())
        );

        // Test date_trunc with right tuple column
        let right_trunc = DateTimeExpression::new(
            DateTimeOperation::DateTrunc,
            vec![Arc::new(Expression::ColumnRef(ColumnRefExpression::new(
                1,
                0,
                Column::new("right_ts", TypeId::Timestamp),
                vec![],
            )))],
            Some(DateTimeField::Month),
            Column::new("result", TypeId::VarChar),
        );

        let result = right_trunc
            .evaluate_join(&left_tuple, &left_schema, &right_tuple, &right_schema)
            .unwrap();
        println!("Right result: {:?}", result.get_val());
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-02-01T00:00:00Z".to_string())
        );
    }

    #[test]
    fn test_datetime_interval_operations() {
        let schema = create_test_schema();
        let tuple = create_test_tuple();

        // Test timestamp + interval
        let add_interval = DateTimeExpression::new(
            DateTimeOperation::Add,
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new("2024-01-01T00:00:00Z"),
                    Column::new("timestamp", TypeId::VarChar),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new("P1D"),
                    Column::new("interval", TypeId::VarChar),
                    vec![],
                ))), // ISO 8601 duration format for 1 day
            ],
            None,
            Column::new("result", TypeId::VarChar),
        );

        let result = add_interval.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2024-01-02T00:00:00Z".to_string())
        );

        // Test timestamp - interval
        let sub_interval = DateTimeExpression::new(
            DateTimeOperation::Subtract,
            vec![
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new("2024-01-01T00:00:00Z"),
                    Column::new("timestamp", TypeId::VarChar),
                    vec![],
                ))),
                Arc::new(Expression::Constant(ConstantExpression::new(
                    Value::new("P1D"),
                    Column::new("interval", TypeId::VarChar),
                    vec![],
                ))), // ISO 8601 duration format for 1 day
            ],
            None,
            Column::new("result", TypeId::VarChar),
        );

        let result = sub_interval.evaluate(&tuple, &schema).unwrap();
        assert_eq!(
            result.get_val(),
            &Val::VarLen("2023-12-31T00:00:00Z".to_string())
        );
    }
}
