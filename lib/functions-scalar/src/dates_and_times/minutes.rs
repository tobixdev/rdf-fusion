use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::{DateTime, Integer};

#[derive(Debug)]
pub struct MinutesSparqlOp;

impl Default for MinutesSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl MinutesSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for MinutesSparqlOp {
}

impl UnarySparqlOp for MinutesSparqlOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.minute().into())
    }
}
