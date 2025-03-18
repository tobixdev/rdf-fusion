use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Boolean, TermRef};

#[derive(Debug)]
pub struct BoundRdfOp {}

impl BoundRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for BoundRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, _: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        Ok(true.into())
    }

    fn evaluate_error<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Ok(false.into())
    }
}
