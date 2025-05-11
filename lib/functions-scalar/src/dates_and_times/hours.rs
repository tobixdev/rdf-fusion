use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::{DateTime, Integer};

#[derive(Debug)]
pub struct HoursSparqlOp;

impl Default for HoursSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl HoursSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for HoursSparqlOp {}

impl UnarySparqlOp for HoursSparqlOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.hour().into())
    }
}
