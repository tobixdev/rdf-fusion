use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Boolean, InternalTermRef};

#[derive(Debug)]
pub struct IsLiteralRdfOp;

impl Default for IsLiteralRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsLiteralRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for IsLiteralRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = !matches!(
            value,
            InternalTermRef::NamedNode(_) | InternalTermRef::BlankNode(_)
        );
        Ok(result.into())
    }
}
