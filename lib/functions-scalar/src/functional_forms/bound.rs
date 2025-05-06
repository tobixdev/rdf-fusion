use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use graphfusion_model::{Boolean, TermValueRef, TermRef};

#[derive(Debug)]
pub struct BoundSparqlOp;

impl Default for BoundSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl BoundSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for BoundSparqlOp {
    fn name(&self) -> &str {
        "bound"
    }
}

impl UnaryTermValueOp for BoundSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, _: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(true.into())
    }

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        Ok(false.into())
    }
}

impl UnaryRdfTermOp for BoundSparqlOp {
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, _: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(true.into())
    }

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        Ok(false.into())
    }
}
