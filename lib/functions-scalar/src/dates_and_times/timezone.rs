use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{DateTime, DayTimeDuration, ThinError};

#[derive(Debug)]
pub struct TimezoneRdfOp;

impl Default for TimezoneRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl TimezoneRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for TimezoneRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = DayTimeDuration;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        value.timezone().ok_or(ThinError::Expected)
    }
}
