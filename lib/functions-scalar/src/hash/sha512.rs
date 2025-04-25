use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{OwnedStringLiteral, SimpleLiteralRef};
use md5::Digest;
use sha2::Sha512;

#[derive(Debug)]
pub struct Sha512RdfOp;

impl Default for Sha512RdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha512RdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for Sha512RdfOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Sha512::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
