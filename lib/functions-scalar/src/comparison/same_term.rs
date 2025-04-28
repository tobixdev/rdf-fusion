use crate::{ScalarBinaryRdfOp, ThinResult};
use model::{Boolean, InternalTermRef};

#[derive(Debug)]
pub struct SameTermRdfOp;

impl Default for SameTermRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SameTermRdfOp {
    pub fn new() -> Self {
        Self
    }
}

impl ScalarBinaryRdfOp for SameTermRdfOp {
    type ArgLhs<'data> = InternalTermRef<'data>;
    type ArgRhs<'data> = InternalTermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let result = match (lhs, rhs) {
            (InternalTermRef::NamedNode(l), InternalTermRef::NamedNode(r)) => l == r,
            (InternalTermRef::BlankNode(l), InternalTermRef::BlankNode(r)) => l == r,
            (InternalTermRef::BooleanLiteral(l), InternalTermRef::BooleanLiteral(r)) => l == r,
            (InternalTermRef::NumericLiteral(l), InternalTermRef::NumericLiteral(r)) => l == r,
            (InternalTermRef::SimpleLiteral(l), InternalTermRef::SimpleLiteral(r)) => l == r,
            (
                InternalTermRef::LanguageStringLiteral(l),
                InternalTermRef::LanguageStringLiteral(r),
            ) => l == r,
            (InternalTermRef::DateTimeLiteral(l), InternalTermRef::DateTimeLiteral(r)) => l == r,
            (InternalTermRef::DateLiteral(l), InternalTermRef::DateLiteral(r)) => l == r,
            (InternalTermRef::TimeLiteral(l), InternalTermRef::TimeLiteral(r)) => l == r,
            (InternalTermRef::DurationLiteral(l), InternalTermRef::DurationLiteral(r)) => l == r,
            (
                InternalTermRef::YearMonthDurationLiteral(l),
                InternalTermRef::YearMonthDurationLiteral(r),
            ) => l == r,
            (
                InternalTermRef::DayTimeDurationLiteral(l),
                InternalTermRef::DayTimeDurationLiteral(r),
            ) => l == r,
            (InternalTermRef::TypedLiteral(l), InternalTermRef::TypedLiteral(r)) => l == r,
            _ => false,
        };
        Ok(result.into())
    }
}
