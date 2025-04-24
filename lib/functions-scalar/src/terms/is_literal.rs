use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Boolean, TermRef};

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
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let result = matches!(value, TermRef::NamedNode(_) | TermRef::BlankNode(_));
        Ok(result.into())
    }
}
