use crate::{ScalarBinaryRdfOp, ThinResult};
use model::{Boolean, InternalTermRef, ThinError};
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
    type ArgLhs<'data> = InternalTermRef<'data>;
    type ArgRhs<'data> = InternalTermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        match (lhs, rhs) {
            // Same term are also equal.
            (InternalTermRef::TypedLiteral(l), InternalTermRef::TypedLiteral(r)) if l == r => {
                Ok(true.into())
            }
            // Cannot say anything about unsupported typed literals that are not the same term.
            (_, InternalTermRef::TypedLiteral(_)) | (InternalTermRef::TypedLiteral(_), _) => {
                ThinError::expected()
            }
            // For numerics, compare values.
            (InternalTermRef::NumericLiteral(lhs), InternalTermRef::NumericLiteral(rhs)) => {
                Ok((lhs.partial_cmp(&rhs) == Some(Ordering::Equal)).into())
            }
            // For dates & times, compare values.
            (InternalTermRef::DateTimeLiteral(lhs), InternalTermRef::DateTimeLiteral(rhs)) => {
                Ok((lhs == rhs).into())
            }
            (InternalTermRef::DateLiteral(lhs), InternalTermRef::DateLiteral(rhs)) => {
                Ok((lhs == rhs).into())
            }
            (InternalTermRef::TimeLiteral(lhs), InternalTermRef::TimeLiteral(rhs)) => {
                Ok((lhs == rhs).into())
            }
            // For durations, compare values.
            (InternalTermRef::DurationLiteral(lhs), InternalTermRef::DurationLiteral(rhs)) => {
                Ok((lhs == rhs).into())
            }
            (
                InternalTermRef::YearMonthDurationLiteral(lhs),
                InternalTermRef::YearMonthDurationLiteral(rhs),
            ) => Ok((lhs == rhs).into()),
            (
                InternalTermRef::DayTimeDurationLiteral(lhs),
                InternalTermRef::DayTimeDurationLiteral(rhs),
            ) => Ok((lhs == rhs).into()),
            // LanguageStringLiterals may not have a defined comparison.
            (
                InternalTermRef::LanguageStringLiteral(l),
                InternalTermRef::LanguageStringLiteral(r),
            ) => l
                .partial_cmp(&r)
                .map(|r| r == Ordering::Equal)
                .map(Into::into)
                .ok_or(ThinError::Expected),
            // Otherwise compare for equality.
            _ => Ok((lhs == rhs).into()),
        }
    }
}
