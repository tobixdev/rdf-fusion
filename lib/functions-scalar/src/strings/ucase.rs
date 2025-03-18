use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{TermRef, StringLiteral};

#[derive(Debug)]
pub struct UCaseRdfOp {}

impl UCaseRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for UCaseRdfOp {
    type Arg<'data> = StringLiteral<'data>;
    type Result<'data> = TermRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        (&value.len().to_string().to_uppercase(), value.1);
        todo!()
    }
}
