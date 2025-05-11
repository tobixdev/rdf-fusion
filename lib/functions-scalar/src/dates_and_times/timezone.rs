use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::{DateTime, DayTimeDuration, ThinError};

#[derive(Debug)]
pub struct TimezoneSparqlOp;

impl Default for TimezoneSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl TimezoneSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for TimezoneSparqlOp {}

impl UnarySparqlOp for TimezoneSparqlOp {
    type Arg<'data> = DateTime;
    type Result<'data> = DayTimeDuration;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        value.timezone().ok_or(ThinError::Expected)
    }
}
