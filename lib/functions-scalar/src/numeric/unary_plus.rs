use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::Numeric;

#[derive(Debug)]
pub struct UnaryPlusRdfOp;

impl Default for UnaryPlusRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UnaryPlusRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for UnaryPlusRdfOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        Ok(value)
    }
}
