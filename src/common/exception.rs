use crate::common::config::{FrameId, PageId};
use std::error::Error;
use std::fmt;
use std::fmt::{Display, Formatter};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DeletePageError {
    #[error("Page {0} not found in page table")]
    PageNotFound(PageId),
    #[error("Frame {0} not found in pages array")]
    FrameNotFound(FrameId),
    #[error("Failed to acquire lock: {0}")]
    LockError(String),
}

#[derive(Debug)]
pub enum FlushError {
    IoError(std::io::Error),
    PageNotFound,
    PageNotInTable,
}

#[derive(Debug)]
pub enum PageError {
    NoPageReference,
    LockError,
    TypeTooLarge,
    InvalidCast,
    DataTooLarge { data_size: usize, remaining_space: usize },
    InvalidOffset { offset: usize, page_size: usize },
    InvalidOperation,
}

#[derive(Error, Debug)]
pub enum ExpressionError {
    #[error("Invalid type for StringExpression: expected VARCHAR")]
    InvalidStringExpressionType,
    #[error("Evaluation error: {0}")]
    EvaluationError(String),
    #[error("Array Expression error: {0}")]
    Array(ArrayExpressionError),
    #[error("Arithmetic error: {0}")]
    ArithmeticError(ArithmeticExpressionError)
}

#[derive(Debug, Error)]
pub enum ArrayExpressionError {
    #[error("Vector value can only be constructed from decimal type")]
    NonDecimalType,
    #[error("Failed to evaluate child expression: {0}")]
    ChildEvaluationError(String),
    #[error("Failed to convert float to integer: {0}")]
    FloatToIntConversionError(f64),
}

#[derive(Debug, Error)]
pub enum ArithmeticExpressionError {
    #[error("Unknown")]
    Unknown,
    #[error("Division by zero")]
    DivisionByZero
}


#[derive(Debug)]
pub struct PageGuardError;

impl Error for PageError {}

impl Error for PageGuardError {}

impl Display for PageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PageError::InvalidOffset { offset, page_size } => {
                write!(f, "Attempt to write out of bounds data at offset {} with length {}", offset, page_size)
            }
            PageError::DataTooLarge { data_size, remaining_space } => {
                write!(f, "Attempt to write out of bounds data at offset {} with length {}", data_size, remaining_space)
            }
            PageError::NoPageReference => {
                write!(f, "No Page Reference")
            }
            PageError::LockError => {
                write!(f, "Lock Error")
            }
            PageError::TypeTooLarge => {
                write!(f, "Type Too Large")
            }
            PageError::InvalidCast => {
                write!(f, "Invalid Cast")
            }
            PageError::InvalidOperation => {
                write!(f, "Invalid Operation")
            }
        }
    }
}

impl Display for PageGuardError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Page guard error")
    }
}
