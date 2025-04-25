use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{DateTime, Decimal};

#[derive(Debug)]
pub struct SecondsRdfOp;

impl Default for SecondsRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SecondsRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for SecondsRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Decimal;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.second())
    }
}
