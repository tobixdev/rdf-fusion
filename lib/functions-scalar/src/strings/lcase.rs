use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{OwnedStringLiteral, StringLiteralRef};

#[derive(Debug)]
pub struct LCaseRdfOp {}

impl LCaseRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for LCaseRdfOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        Ok(OwnedStringLiteral(
            value.0.to_string().to_lowercase(),
            value.1.map(String::from),
        ))
    }
}
