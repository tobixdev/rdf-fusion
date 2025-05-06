use crate::{BinaryRdfTermOp, BinaryTermValueOp, SparqlOp, ThinResult};
use graphfusion_model::{Boolean, TermValueRef, TermRef};

#[derive(Debug)]
pub struct SameTermSparqlOp;

impl Default for SameTermSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SameTermSparqlOp {
    pub fn new() -> Self {
        Self
    }
}

impl SparqlOp for SameTermSparqlOp {
    fn name(&self) -> &str {
        "sameTerm"
    }
}

impl BinaryTermValueOp for SameTermSparqlOp {
    type ArgLhs<'data> = TermValueRef<'data>;
    type ArgRhs<'data> = TermValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        // TODO: fix PartialEq for RdfValue to be SameTerm
        Ok((lhs == rhs).into())
    }
}

impl BinaryRdfTermOp for SameTermSparqlOp {
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: TermRef<'data>,
        rhs: TermRef<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        Ok((lhs == rhs).into())
    }
}
