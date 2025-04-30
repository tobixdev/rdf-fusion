use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{OwnedStringLiteral, StringLiteralRef};

#[derive(Debug)]
pub struct LCaseRdfOp;

impl Default for LCaseRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LCaseRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for LCaseRdfOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(OwnedStringLiteral(
            value.0.to_owned().to_lowercase(),
            value.1.map(String::from),
        ))
    }
}
