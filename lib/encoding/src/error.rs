use datafusion::arrow::error::ArrowError;
use graphfusion_model::{ParseDateTimeError, ParseDecimalError, ParseDurationError};
use std::error::Error;
use std::num::{ParseFloatError, ParseIntError};
use std::str::ParseBoolError;

#[derive(Debug, thiserror::Error)]
pub enum LiteralEncodingError {
    #[error("Error while writing to the arrow arrays.")]
    Arrow(#[from] ArrowError),
    #[error("There was an error while parsing the query solutions")]
    ParsingError(#[from] Box<dyn Error>),
}

impl From<ParseIntError> for LiteralEncodingError {
    fn from(error: ParseIntError) -> Self {
        LiteralEncodingError::ParsingError(Box::new(error))
    }
}

impl From<ParseFloatError> for LiteralEncodingError {
    fn from(error: ParseFloatError) -> Self {
        LiteralEncodingError::ParsingError(Box::new(error))
    }
}

impl From<ParseBoolError> for LiteralEncodingError {
    fn from(error: ParseBoolError) -> Self {
        LiteralEncodingError::ParsingError(Box::new(error))
    }
}

impl From<ParseDecimalError> for LiteralEncodingError {
    fn from(error: ParseDecimalError) -> Self {
        LiteralEncodingError::ParsingError(Box::new(error))
    }
}

impl From<ParseDurationError> for LiteralEncodingError {
    fn from(error: ParseDurationError) -> Self {
        LiteralEncodingError::ParsingError(Box::new(error))
    }
}

impl From<ParseDateTimeError> for LiteralEncodingError {
    fn from(error: ParseDateTimeError) -> Self {
        LiteralEncodingError::ParsingError(Box::new(error))
    }
}
