use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Boolean, InternalTermRef};

#[derive(Debug)]
pub struct BoundRdfOp;

impl Default for BoundRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl BoundRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for BoundRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, _: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(true.into())
    }

    fn evaluate_error<'data>(&self) -> ThinResult<Self::Result<'data>> {
        Ok(false.into())
    }
}
