use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{DateTime, DayTimeDuration};

#[derive(Debug)]
pub struct TimezoneRdfOp {}

impl TimezoneRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for TimezoneRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = DayTimeDuration;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        Ok(value.timezone().ok_or(())?)
    }
}
