use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{Boolean, TermRef};

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
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TermRef::NamedNode(_));
        Ok(result.into())
    }
}
