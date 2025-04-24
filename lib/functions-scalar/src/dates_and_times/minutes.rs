use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{DateTime, Integer};

#[derive(Debug)]
pub struct MinutesRdfOp;

impl Default for MinutesRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl MinutesRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for MinutesRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        Ok(value.minute().into())
    }
}
