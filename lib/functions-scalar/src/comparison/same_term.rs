use crate::{BinarySparqlOp, SparqlOp, ThinResult};
use graphfusion_model::{Boolean, TypedValueRef};

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
    type ArgLhs<'data> = TypedValueRef<'data>;
    type ArgRhs<'data> = TypedValueRef<'data>;
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
