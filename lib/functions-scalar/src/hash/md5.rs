use crate::{ScalarUnaryRdfOp, ThinResult};
use md5::{Digest, Md5};
use model::{OwnedStringLiteral, SimpleLiteralRef};

#[derive(Debug)]
pub struct Md5RdfOp;

impl Default for Md5RdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5RdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for Md5RdfOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Md5::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
