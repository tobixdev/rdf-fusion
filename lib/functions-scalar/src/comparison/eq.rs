use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{Boolean, TermRef};
use std::cmp::Ordering;

#[derive(Debug)]
pub struct EqRdfOp {}

impl EqRdfOp {
    pub fn new() -> Self {
        Self {}
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
    ) -> RdfOpResult<Self::Result<'data>> {
        match (lhs, rhs) {
            // Same term are also equal.
            (TermRef::TypedLiteral(l), TermRef::TypedLiteral(r)) if l == r => Ok(true.into()),
            // Cannot say anything about unsupported typed literals that are not the same term.
            (TermRef::TypedLiteral(_), _) => Err(()),
            (_, TermRef::TypedLiteral(_)) => Err(()),
            // For numerics, compare values.
            (TermRef::NumericLiteral(lhs), TermRef::NumericLiteral(rhs)) => {
                Ok((lhs.cmp(&rhs) == Ordering::Equal).into())
            }
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
