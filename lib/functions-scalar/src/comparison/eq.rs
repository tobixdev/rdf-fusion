use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::TermRef;
use std::cmp::Ordering;

pub struct EqRdfOp {}

impl ScalarBinaryRdfOp for EqRdfOp {
    type ArgLhs<'data> = TermRef<'data>;
    type ArgRhs<'data> = TermRef<'data>;
    type Result<'data> = TermRef<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        let result = match (lhs, rhs) {
            (TermRef::NamedNode(l), TermRef::NamedNode(r)) => l == r,
            (TermRef::BlankNode(l), TermRef::BlankNode(r)) => l == r,
            (TermRef::Boolean(l), TermRef::Boolean(r)) => l == r,
            (TermRef::Numeric(l), TermRef::Numeric(r)) => l.cmp(&r) == Ordering::Equal,
            (TermRef::SimpleLiteral(l), TermRef::SimpleLiteral(r)) => l == r,
            (TermRef::LanguageString(l), TermRef::LanguageString(r)) => l == r,
            (TermRef::TypedLiteral(l), TermRef::TypedLiteral(r)) => l == r,
            _ => false,
        };
        Ok(TermRef::Boolean(result.into()))
    }
}
