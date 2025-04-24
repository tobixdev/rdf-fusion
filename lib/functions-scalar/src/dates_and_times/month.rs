use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{DateTime, Integer};

#[derive(Debug)]
pub struct MonthRdfOp;

impl Default for MonthRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl MonthRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for MonthRdfOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        Ok(value.month().into())
    }
}
