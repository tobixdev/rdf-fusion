use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{DateTime, Integer};

#[derive(Debug)]
pub struct YearRdfOp;

impl Default for YearRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl YearRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for YearRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        Ok(value.year().into())
    }
}
