use crate::{UnaryTermValueOp, ThinResult, SparqlOp};
use graphfusion_model::Numeric;

#[derive(Debug)]
pub struct UnaryPlusSparqlOp;

impl Default for UnaryPlusSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UnaryPlusSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for UnaryPlusSparqlOp {
    fn name(&self) -> &str {
        "plus"
    }
}

impl UnaryTermValueOp for UnaryPlusSparqlOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value)
    }
}
