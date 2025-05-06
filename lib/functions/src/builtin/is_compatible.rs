use crate::value_encoding::RdfTermValueEncoding;
use graphfusion_functions_scalar::{BinaryTermValueOp, SparqlOp};
use datafusion::logical_expr::{Signature, TypeSignature, Volatility};
use graphfusion_model::{Boolean, RdfTermValueRef, ThinResult};

#[derive(Debug)]
pub struct IsCompatibleSparqlOp {
    signature: Signature,
}

impl Default for IsCompatibleSparqlOp {
    fn default() -> Self {
        Self {
            signature: Signature::new(
                TypeSignature::Exact(vec![RdfTermValueEncoding::datatype(); 2]),
                Volatility::Immutable,
            ),
        }
    }
}

impl SparqlOp for IsCompatibleSparqlOp {
    fn name(&self) -> &str {
        "is_compatible"
    }
}

impl BinaryTermValueOp for IsCompatibleSparqlOp {
    type ArgLhs<'data> = RdfTermValueRef<'data>;
    type ArgRhs<'data> = RdfTermValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        let is_compatible = match (lhs, rhs) {
            (RdfTermValueRef::BlankNode(lhs), RdfTermValueRef::BlankNode(rhs)) => lhs == rhs,
            (RdfTermValueRef::NamedNode(lhs), RdfTermValueRef::NamedNode(rhs)) => lhs == rhs,
            (RdfTermValueRef::BooleanLiteral(lhs), RdfTermValueRef::BooleanLiteral(rhs)) => {
                lhs == rhs
            }
            (RdfTermValueRef::NumericLiteral(lhs), RdfTermValueRef::NumericLiteral(rhs)) => {
                lhs == rhs
            }
            (RdfTermValueRef::SimpleLiteral(lhs), RdfTermValueRef::SimpleLiteral(rhs)) => {
                lhs == rhs
            }
            (
                RdfTermValueRef::LanguageStringLiteral(lhs),
                RdfTermValueRef::LanguageStringLiteral(rhs),
            ) => lhs == rhs,
            (RdfTermValueRef::OtherLiteral(lhs), RdfTermValueRef::OtherLiteral(rhs)) => lhs == rhs,
            _ => false,
        };
        Ok(is_compatible.into())
    }

    fn evaluate_error<'data>(
        &self,
        _lhs: ThinResult<Self::ArgLhs<'data>>,
        _rhs: ThinResult<Self::ArgRhs<'data>>,
    ) -> ThinResult<Self::Result<'data>> {
        // At least one side is an error, therefore the terms are compatible.
        Ok(true.into())
    }
}
