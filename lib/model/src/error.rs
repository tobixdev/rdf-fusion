use crate::{
    DateTimeOverflowError, OppositeSignInDurationComponentsError, ParseDateTimeError,
    ParseDecimalError, TooLargeForDecimalError, TooLargeForIntError, TooLargeForIntegerError,
};
use oxiri::IriParseError;
use oxrdf::BlankNodeIdParseError;
use std::fmt::Debug;
use std::num::{ParseFloatError, ParseIntError, TryFromIntError};
use std::str::ParseBoolError;
use std::string::FromUtf8Error;
use thiserror::Error;

// TODO ThinResult = Option<T> in the future

/// A light-weight result, mainly used for SPARQL operations.
pub type ThinResult<T> = Result<T, ThinError>;

/// A thin error type that indicates whether an issue is expected (e.g., a SPARQL error) or if this
/// error is unexpected and should not happen (i.e., programming errors).
#[derive(Clone, Copy, Debug, Default, Error, PartialEq, Eq)]
pub enum ThinError {
    #[default]
    #[error("Expected error")]
    Expected,
    // TODO: After re-thinking, some should probably panic and some we should use a different type.
    // It's too easy to forget handling the Not Expected error.
    #[error("An internal error occurred. This is most likely a bug in RdfFusion. Reason: {0}")]
    InternalError(&'static str),
}

impl ThinError {
    /// Creates an expected error. This should be used for fallible operations, such as SPARQL
    /// operations that can return an error.
    pub fn expected<T>() -> ThinResult<T> {
        Err(ThinError::Expected)
    }

    /// Creates an internal error. This should be used if an error likely indicates a bug.
    pub fn internal_error<T>(expected: &'static str) -> ThinResult<T> {
        Err(ThinError::InternalError(expected))
    }
}

macro_rules! implement_from {
    ($t:ty) => {
        impl From<$t> for ThinError {
            fn from(_: $t) -> Self {
                ThinError::Expected
            }
        }
    };
}

implement_from!(TooLargeForDecimalError);
implement_from!(TooLargeForIntegerError);
implement_from!(TooLargeForIntError);
implement_from!(ParseBoolError);
implement_from!(ParseIntError);
implement_from!(ParseFloatError);
implement_from!(ParseDecimalError);
implement_from!(ParseDateTimeError);
implement_from!(BlankNodeIdParseError);
implement_from!(IriParseError);
implement_from!(TryFromIntError);
implement_from!(DateTimeOverflowError);
implement_from!(OppositeSignInDurationComponentsError);
implement_from!(FromUtf8Error);
