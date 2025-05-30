use crate::{BinarySparqlOp, SparqlOp, ThinResult};
use rdf_fusion_model::{Boolean, TermRef};

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

impl SparqlOp for SameTermSparqlOp {}

impl BinarySparqlOp for SameTermSparqlOp {
    type ArgLhs<'data> = TermRef<'data>;
    type ArgRhs<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        Ok((lhs == rhs).into())
    }
}
