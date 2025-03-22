use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{Integer, StringLiteralRef};

#[derive(Debug)]
pub struct StrLenRdfOp {}

impl StrLenRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for StrLenRdfOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let value: i64 = value.len().try_into().map_err(|_| ())?;
        Ok(value.into())
    }
}
