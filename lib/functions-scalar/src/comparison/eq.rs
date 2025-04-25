use crate::{ScalarBinaryRdfOp, ThinResult};
use model::{Boolean, TermRef, ThinError};
use std::cmp::Ordering;

#[derive(Debug)]
pub struct EqRdfOp;

impl Default for EqRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl EqRdfOp {
    pub fn new() -> Self {
        Self
    }
}

impl ScalarBinaryRdfOp for EqRdfOp {
    type ArgLhs<'data> = TermRef<'data>;
    type ArgRhs<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        match (lhs, rhs) {
            // Same term are also equal.
            (TermRef::TypedLiteral(l), TermRef::TypedLiteral(r)) if l == r => Ok(true.into()),
            // Cannot say anything about unsupported typed literals that are not the same term.
            (_, TermRef::TypedLiteral(_)) | (TermRef::TypedLiteral(_), _) => ThinError::expected(),
            // For numerics, compare values.
            (TermRef::NumericLiteral(lhs), TermRef::NumericLiteral(rhs)) => {
                Ok((lhs.partial_cmp(&rhs) == Some(Ordering::Equal)).into())
            }
            // For dates & times, compare values.
            (TermRef::DateTimeLiteral(lhs), TermRef::DateTimeLiteral(rhs)) => {
                Ok((lhs == rhs).into())
            }
            (TermRef::DateLiteral(lhs), TermRef::DateLiteral(rhs)) => Ok((lhs == rhs).into()),
            (TermRef::TimeLiteral(lhs), TermRef::TimeLiteral(rhs)) => Ok((lhs == rhs).into()),
            // For durations, compare values.
            (TermRef::DurationLiteral(lhs), TermRef::DurationLiteral(rhs)) => {
                Ok((lhs == rhs).into())
            }
            (TermRef::YearMonthDurationLiteral(lhs), TermRef::YearMonthDurationLiteral(rhs)) => {
                Ok((lhs == rhs).into())
            }
            (TermRef::DayTimeDurationLiteral(lhs), TermRef::DayTimeDurationLiteral(rhs)) => {
                Ok((lhs == rhs).into())
            }
            // Otherwise compare for equality.
            _ => Ok((lhs == rhs).into()),
        }
    }
}
