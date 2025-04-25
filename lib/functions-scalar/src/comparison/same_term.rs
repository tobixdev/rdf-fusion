use crate::{ScalarBinaryRdfOp, ThinResult};
use datamodel::{Boolean, TermRef};

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
    type ArgLhs<'data> = TermRef<'data>;
    type ArgRhs<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let result = match (lhs, rhs) {
            (TermRef::NamedNode(l), TermRef::NamedNode(r)) => l == r,
            (TermRef::BlankNode(l), TermRef::BlankNode(r)) => l == r,
            (TermRef::BooleanLiteral(l), TermRef::BooleanLiteral(r)) => l == r,
            (TermRef::NumericLiteral(l), TermRef::NumericLiteral(r)) => l == r,
            (TermRef::SimpleLiteral(l), TermRef::SimpleLiteral(r)) => l == r,
            (TermRef::LanguageStringLiteral(l), TermRef::LanguageStringLiteral(r)) => l == r,
            (TermRef::DateTimeLiteral(l), TermRef::DateTimeLiteral(r)) => l == r,
            (TermRef::DateLiteral(l), TermRef::DateLiteral(r)) => l == r,
            (TermRef::TimeLiteral(l), TermRef::TimeLiteral(r)) => l == r,
            (TermRef::DurationLiteral(l), TermRef::DurationLiteral(r)) => l == r,
            (TermRef::YearMonthDurationLiteral(l), TermRef::YearMonthDurationLiteral(r)) => l == r,
            (TermRef::DayTimeDurationLiteral(l), TermRef::DayTimeDurationLiteral(r)) => l == r,
            (TermRef::TypedLiteral(l), TermRef::TypedLiteral(r)) => l == r,
            _ => false,
        };
        Ok(result.into())
    }
}
