use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{DateTime, Integer};

#[derive(Debug)]
pub struct DayRdfOp;

impl Default for DayRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl DayRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for DayRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.day().into())
    }
}
