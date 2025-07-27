use crate::{
    DateTimeOverflowError, OppositeSignInDurationComponentsError, ParseDateTimeError,
    ParseDecimalError, TooLargeForDecimalError, TooLargeForIntError, TooLargeForIntegerError,
};
use oxiri::IriParseError;
use oxrdf::BlankNodeIdParseError;
use std::fmt::{Debug, Display, Formatter};
use std::num::{ParseFloatError, ParseIntError, TryFromIntError};
use std::str::ParseBoolError;
use std::string::FromUtf8Error;
use thiserror::Error;

/// A light-weight result, mainly used for SPARQL operations.
pub type ThinResult<T> = Result<T, ThinError>;

/// A thin error type that indicates an *expected* failure without any reason.
///
/// In SPARQL, many operations can fail. For example, because the input value had a different data
/// type. However, these errors are expected and are part of the query evaluation. As all of these
/// "expected" errors are treated equally in the query evaluation, we do not need to store a reason.
#[derive(Clone, Copy, Debug, Default, Error, PartialEq, Eq)]
pub struct ThinError {}

impl ThinError {
    /// Creates a result with a [ThinError].
    pub fn expected<T>() -> ThinResult<T> {
        Err(ThinError::default())
    }
}

impl Display for ThinError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str("An expected error occurred.")
    }
}

macro_rules! implement_from {
    ($t:ty) => {
        impl From<$t> for ThinError {
            fn from(_: $t) -> Self {
                ThinError::default()
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
