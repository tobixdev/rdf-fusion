use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::{Boolean, TypedValueRef};

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

impl SparqlOp for BoundSparqlOp {}

impl UnarySparqlOp for BoundSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, _: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(true.into())
    }

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        Ok(false.into())
    }
}
