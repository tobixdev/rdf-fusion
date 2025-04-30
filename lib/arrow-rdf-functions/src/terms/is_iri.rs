use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Boolean, InternalTermRef};

#[derive(Debug)]
pub struct IsIriRdfOp;

impl Default for IsIriRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsIriRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for IsIriRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, InternalTermRef::NamedNode(_));
        Ok(result.into())
    }
}
