use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
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
    fn name(&self) -> &str {
        "minutes"
    }
}

impl UnaryTermValueOp for MinutesSparqlOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.minute().into())
    }
}
