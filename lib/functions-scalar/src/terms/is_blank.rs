use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Boolean, InternalTermRef};

#[derive(Debug)]
pub struct IsBlankRdfOp;

impl Default for IsBlankRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsBlankRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for IsBlankRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, InternalTermRef::BlankNode(_));
        Ok(result.into())
    }
}
