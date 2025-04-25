use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{DateTime, Integer};

#[derive(Debug)]
pub struct HoursRdfOp;

impl Default for HoursRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl HoursRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for HoursRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.hour().into())
    }
}
