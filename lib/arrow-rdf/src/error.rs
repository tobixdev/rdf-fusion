use datafusion::arrow::error::ArrowError;
use std::error::Error;
use std::num::ParseIntError;

pub enum TermEncodingError {
    Arrow(ArrowError),
    ParsingError(Box<dyn Error>),
}

impl From<ArrowError> for TermEncodingError {
    fn from(error: ArrowError) -> Self {
        TermEncodingError::Arrow(error)
    }
}

impl From<ParseIntError> for TermEncodingError {
    fn from(error: ParseIntError) -> Self {
        TermEncodingError::ParsingError(Box::new(error))
    }
}
