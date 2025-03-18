use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Boolean, TermRef};

#[derive(Debug)]
pub struct IsBlankRdfOp {}

impl IsBlankRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for IsBlankRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let result = match value {
            TermRef::BlankNode(_) => true,
            _ => false,
        };
        Ok(result.into())
    }
}
