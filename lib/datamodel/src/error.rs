use crate::{
    DateTimeOverflowError, ParseDecimalError, TooLargeForDecimalError, TooLargeForIntError,
    TooLargeForIntegerError,
};
use std::error::Error;
use std::fmt::{Debug, Display, Formatter};
use std::num::{ParseFloatError, ParseIntError};

/// A light-weight result for RDF operations.
pub type RdfOpResult<T> = Result<T, RdfOpError>;

/// An empty error type for RDF operations. We do not want to add details to the error, as they are
/// all encoded as NULLs in the solution sequence.
#[derive(Clone, Copy)]
pub struct RdfOpError;

impl Debug for RdfOpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RdfOpError")
    }
}

impl Display for RdfOpError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "RdfOpError")
    }
}

impl Error for RdfOpError {}

macro_rules! implement_from {
    ($t:ty) => {
        impl From<$t> for RdfOpError {
            fn from(_: $t) -> Self {
                RdfOpError
            }
        }
    };
}

implement_from!(());
implement_from!(ParseDecimalError);
implement_from!(TooLargeForDecimalError);
implement_from!(TooLargeForIntegerError);
implement_from!(TooLargeForIntError);
implement_from!(ParseIntError);
implement_from!(ParseFloatError);
implement_from!(DateTimeOverflowError);
