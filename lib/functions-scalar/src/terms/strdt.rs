use crate::{BinarySparqlOp, SparqlOp, ThinResult};
use rdf_fusion_model::{LiteralRef, NamedNodeRef, SimpleLiteralRef};

#[derive(Debug)]
pub struct StrDtSparqlOp;

impl Default for StrDtSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrDtSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrDtSparqlOp {}

impl BinarySparqlOp for StrDtSparqlOp {
    type ArgLhs<'lhs> = SimpleLiteralRef<'lhs>;
    type ArgRhs<'rhs> = NamedNodeRef<'rhs>;
    type Result<'data> = LiteralRef<'data>;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        Ok(LiteralRef::new_typed_literal(lhs.value, rhs))
    }
}
