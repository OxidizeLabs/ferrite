use crate::common::config::{FrameId, PageId};
use crate::types_db::type_id::TypeId;
use serde_json;
use sqlparser::parser::ParserError;
use std::error::Error;
use std::error::Error as StdError;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::io;
use thiserror::Error;

#[derive(Error, Debug, PartialEq)]
pub enum LockError {
    #[error("Attempted to use intention lock on row")]
    AttemptedIntentionLockOnRow,

    #[error("Attempted shared lock in READ_UNCOMMITTED")]
    LockSharedOnReadUncommitted,

    #[error("Attempted lock while transaction is in SHRINKING phase")]
    LockOnShrinking,

    #[error("Required table lock not present")]
    TableLockNotPresent,

    #[error("Incompatible lock upgrade requested")]
    IncompatibleUpgrade,

    #[error("Lock upgrade conflict with another transaction")]
    UpgradeConflict,

    #[error("Attempted to unlock without holding lock")]
    NoLockHeld,

    #[error("Attempted to unlock table before unlocking rows")]
    TableUnlockedBeforeRows,

    #[error("Failed to abort transaction")]
    TransactionAborted,

    #[error("Failed to commit transaction")]
    TransactionCommitted,

    #[error("Timeout Error")]
    Timeout,

    #[error("Intention Lock on Row Error")]
    IntentionLockOnRow,
}

#[derive(Error, Debug)]
pub enum ValuesError {
    #[error("Value count {value_count} is not a multiple of column count {column_count}")]
    InvalidValueCount {
        value_count: usize,
        column_count: usize,
    },

    #[error("Type mismatch for column {column_name}: expected {expected:?}, got {actual:?}")]
    TypeMismatch {
        column_name: String,
        expected: TypeId,
        actual: TypeId,
    },

    #[error("Row index {row_idx} is out of bounds (total rows: {total_rows})")]
    RowIndexOutOfBounds { row_idx: usize, total_rows: usize },

    #[error("Value evaluation failed: {0}")]
    ValueEvaluationError(String),
}

#[derive(Debug, PartialEq)]
pub enum ComparisonError {
    ValueRetrievalError(String),
}

#[derive(Debug)]
pub enum KeyConversionError {
    ColumnNotFound(String),
    OffsetConversionError(String),
    DeserializationError(String),
    OffsetOutOfBounds,
    InvalidOffset,
}

#[derive(Error, Debug)]
pub enum TupleError {
    #[error("Storage buffer is too small")]
    BufferTooSmall,
    #[error("Column not found: {0}")]
    ColumnNotFound(usize),
    #[error("Tuple value count mismatch: expected {expected}, got {actual}")]
    ValueCountMismatch { expected: usize, actual: usize },
    #[error("Key attribute index {attr} is out of bounds (tuple column count: {column_count})")]
    KeyAttrOutOfBounds { attr: usize, column_count: usize },
    #[error("SerializationError: {0}")]
    SerializationError(String),
    #[error("DeserializationError: {0}")]
    DeserializationError(String),
    #[error("Undo log index does not fit in usize: {0}")]
    UndoLogIndexOverflow(u64),
    #[error("Undo log index does not fit in u64: {0}")]
    UndoLogIndexU64Overflow(usize),
    #[error("TupleID Out of Range")]
    OutOfRange,
    #[error("Tuple size mismatch")]
    SizeMismatch,
}

#[derive(Error, Debug)]
pub enum DeletePageError {
    #[error("Page {0} not found in page table")]
    PageNotFound(PageId),
    #[error("Frame {0} not found in pages array")]
    FrameNotFound(FrameId),
    #[error("Failed to acquire lock: {0}")]
    LockError(String),
    #[error("Page pinned - PageID: {0}")]
    PagePinned(PageId),
    #[error("Invalid Frame - FrameID: {0}")]
    InvalidFrame(FrameId),
}

#[derive(Debug)]
pub enum FlushError {
    IoError(io::Error),
    PageNotFound,
    PageNotInTable,
}

#[derive(Debug)]
pub enum PageError {
    NoPageReference,
    LockError,
    TypeTooLarge,
    InvalidCast,
    DataTooLarge {
        data_size: usize,
        remaining_space: usize,
    },
    InvalidOffset {
        offset: usize,
        page_size: usize,
    },
    InvalidOperation,
    TupleInvalid,
    SerializationError,
    DeserializationError,
    OffsetOutOfBounds,
}

#[derive(Debug)]
pub struct PageGuardError;

#[derive(Error, Debug)]
pub enum ExpressionError {
    #[error("Invalid type for StringExpression: expected VARCHAR")]
    InvalidStringExpressionType,
    #[error("Evaluation error: {0}")]
    EvaluationError(String),
    #[error("Array Expression error: {0}")]
    Array(ArrayExpressionError),
    #[error("Arithmetic error: {0}")]
    ArithmeticError(ArithmeticExpressionError),
    #[error("DB error: {0}")]
    DB(DBError),
    #[error("Unsupported operation: {0}")]
    UnsupportedOperation(String),
    #[error("Comparison error: {0}")]
    ComparisonError(ComparisonError),
    #[error("Key conversion error: {0}")]
    KeyConversionError(KeyConversionError),
    #[error("Tuple error: {0}")]
    Tuple(TupleError),
    #[error("Delete page error: {0}")]
    DeletePageError(DeletePageError),
    #[error("Flush error: {0}")]
    FlushError(FlushError),
    #[error("Page error: {0}")]
    PageError(PageError),
    #[error("Page guard error: {0}")]
    PageGuardError(PageGuardError),
    #[error("Invalid Operation error: {0}")]
    InvalidOperation(String),
    #[error("Invalid Tuple Index error: {0}")]
    InvalidTupleIndex(usize),
    #[error("Invalid Column Index error: {0}")]
    InvalidColumnIndex(usize),
    #[error("Invalid Column Reference error: {0}")]
    InvalidColumnReference(String),
    #[error("TypeMismatch")]
    TypeMismatch { expected: TypeId, actual: TypeId },
    #[error("CastError: {0}")]
    CastError(String),
    #[error("InvalidCast")]
    InvalidCast { from: TypeId, to: TypeId },
    #[error("InvalidType: {0}")]
    InvalidType(String),
    #[error("Invalid Seed: {0}")]
    InvalidSeed(String),
    #[error("IndexOutOfBounds")]
    IndexOutOfBounds { idx: usize, size: usize },
    #[error("KeyNotFound: {0}")]
    KeyNotFound(String),
    #[error("InvalidReturnType: {0}")]
    InvalidReturnType(String),
    #[error("InvalidExpression: {0}")]
    InvalidExpression(String),
}

#[derive(Debug, Error)]
pub enum ArrayExpressionError {
    #[error("Vector value can only be constructed from decimal type")]
    NonDecimalType,
    #[error("Failed to evaluate child expression: {0}")]
    ChildEvaluationError(String),
    #[error("Failed to convert float to integer: {0}")]
    FloatToIntConversionError(f64),
    #[error("TypeMismatch: {0}")]
    TypeMismatch(String),
}

#[derive(Debug, Error)]
pub enum ArithmeticExpressionError {
    #[error("Unknown")]
    Unknown,
    #[error("Division by zero")]
    DivisionByZero,
    #[error("Overflow")]
    Overflow,
}

#[derive(Debug, Error)]
pub enum DBError {
    #[error("IO error: {0}")]
    Io(String),

    #[error("Lock error: {0}")]
    LockError(String),

    #[error("Transaction error: {0}")]
    Transaction(String),

    #[error("Operation not implemented: {0}")]
    NotImplemented(String),

    #[error("Catalog error: {0}")]
    Catalog(String),

    #[error("Execution error: {0}")]
    Execution(String),

    #[error("Validation error: {0}")]
    Validation(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Plan error: {0}")]
    PlanError(String),

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Optimize error: {0}")]
    OptimizeError(String),

    #[error("Client error: {0}")]
    Client(String),

    #[error("SqlError error: {0}")]
    SqlError(String),

    #[error("Recovery error: {0}")]
    Recovery(String),
}

impl Error for PageError {}

impl Error for PageGuardError {}

impl Error for KeyConversionError {}

impl Display for PageError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PageError::InvalidOffset { offset, page_size } => {
                write!(
                    f,
                    "Attempt to write out of bounds data at offset {} with length {}",
                    offset, page_size
                )
            }
            PageError::DataTooLarge {
                data_size,
                remaining_space,
            } => {
                write!(
                    f,
                    "Attempt to write out of bounds data at offset {} with length {}",
                    data_size, remaining_space
                )
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
            PageError::TupleInvalid => {
                write!(f, "Invalid Tuple")
            }
            PageError::SerializationError => {
                write!(f, "Invalid Serialization")
            }
            PageError::DeserializationError => {
                write!(f, "Invalid Deserialization")
            }
            PageError::OffsetOutOfBounds => {
                write!(f, "OffsetOutOfBounds")
            }
        }
    }
}

impl Display for PageGuardError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "Page guard error")
    }
}

impl Display for KeyConversionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            KeyConversionError::ColumnNotFound(msg) => write!(f, "Column not found: {}", msg),
            KeyConversionError::OffsetConversionError(msg) => {
                write!(f, "Offset conversion error: {}", msg)
            }
            KeyConversionError::DeserializationError(msg) => {
                write!(f, "Deserialization error: {}", msg)
            }
            &KeyConversionError::OffsetOutOfBounds | &KeyConversionError::InvalidOffset => todo!(),
        }
    }
}

impl Display for ComparisonError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            ComparisonError::ValueRetrievalError(msg) => {
                write!(f, "Value retrieval error: {}", msg)
            }
        }
    }
}

impl Display for FlushError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FlushError::IoError(e) => write!(f, "IO error: {}", e),
            FlushError::PageNotFound => write!(f, "Page not found"),
            FlushError::PageNotInTable => write!(f, "Page not in table"),
        }
    }
}

impl From<Box<dyn StdError>> for DBError {
    fn from(error: Box<dyn StdError>) -> Self {
        DBError::Client(error.to_string())
    }
}

impl From<io::Error> for DBError {
    fn from(error: io::Error) -> Self {
        DBError::Io(error.to_string())
    }
}

impl From<ParserError> for DBError {
    fn from(error: ParserError) -> Self {
        DBError::SqlError(error.to_string())
    }
}

impl From<serde_json::Error> for DBError {
    fn from(error: serde_json::Error) -> Self {
        DBError::Internal(format!("JSON error: {}", error))
    }
}

impl From<String> for DBError {
    fn from(error: String) -> Self {
        DBError::Client(error)
    }
}

impl From<&str> for DBError {
    fn from(error: &str) -> Self {
        DBError::Client(error.to_string())
    }
}

impl From<ExpressionError> for DBError {
    fn from(error: ExpressionError) -> Self {
        DBError::Execution(error.to_string())
    }
}

impl From<TupleError> for DBError {
    fn from(error: TupleError) -> Self {
        DBError::Execution(error.to_string())
    }
}

impl From<PageError> for DBError {
    fn from(error: PageError) -> Self {
        DBError::Internal(error.to_string())
    }
}

impl From<FlushError> for DBError {
    fn from(error: FlushError) -> Self {
        DBError::Internal(error.to_string())
    }
}

impl From<DeletePageError> for DBError {
    fn from(error: DeletePageError) -> Self {
        DBError::Internal(error.to_string())
    }
}

impl From<KeyConversionError> for DBError {
    fn from(error: KeyConversionError) -> Self {
        DBError::Internal(error.to_string())
    }
}

impl From<ComparisonError> for DBError {
    fn from(error: ComparisonError) -> Self {
        DBError::Execution(error.to_string())
    }
}

impl From<ArithmeticExpressionError> for DBError {
    fn from(error: ArithmeticExpressionError) -> Self {
        DBError::Execution(error.to_string())
    }
}

impl From<ArrayExpressionError> for DBError {
    fn from(error: ArrayExpressionError) -> Self {
        DBError::Execution(error.to_string())
    }
}
