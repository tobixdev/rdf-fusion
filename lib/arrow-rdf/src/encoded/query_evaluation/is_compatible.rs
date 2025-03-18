use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{Boolean, RdfTerm};

#[derive(Debug)]
pub struct IsCompatibleRdfOp {}

impl IsCompatibleRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for IsCompatibleRdfOp {
    type ArgLhs<'data> = RdfTerm<'data>;
    type ArgRhs<'data> = RdfTerm<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(lhs: &Self::ArgLhs<'data>, rhs: &Self::ArgRhs<'data>) -> RdfOpResult<Self::Result<'data>> {
        let is_compatible = match (lhs, rhs) {
            (RdfTerm::BlankNode(lhs), RdfTerm::BlankNode(rhs)) => lhs == rhs,
            (RdfTerm::NamedNode(lhs), RdfTerm::NamedNode(rhs)) => lhs == rhs,
            (RdfTerm::Boolean(lhs), RdfTerm::Boolean(rhs)) => lhs == rhs,
            (RdfTerm::Numeric(lhs), RdfTerm::Numeric(rhs)) => lhs == rhs,
            (RdfTerm::SimpleLiteral(lhs), RdfTerm::SimpleLiteral(rhs)) => lhs == rhs,
            (RdfTerm::LanguageString(lhs), RdfTerm::LanguageString(rhs)) => lhs == rhs,
            (RdfTerm::TypedLiteral(lhs), RdfTerm::TypedLiteral(rhs)) => lhs == rhs,
            _ => false,
        };
        Ok(is_compatible.into())
    }

    fn evaluate_error() -> RdfOpResult<Self::Result<'static>> {
        // At least one side is an error, therefore the terms are compatible.
        Ok(true.into())
    }
}
