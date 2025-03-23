use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{OwnedStringLiteral, StringLiteralRef};

#[derive(Debug)]
pub struct UCaseRdfOp {}

impl UCaseRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for UCaseRdfOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        Ok(OwnedStringLiteral(
            value.len().to_string().to_uppercase(),
            value.1.map(ToOwned::to_owned),
        ))
    }
}
