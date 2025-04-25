use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{Integer, StringLiteralRef};

#[derive(Debug)]
pub struct StrLenRdfOp;

impl Default for StrLenRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrLenRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for StrLenRdfOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let value: i64 = value.len().try_into()?;
        Ok(value.into())
    }
}
