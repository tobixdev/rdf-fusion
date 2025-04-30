use crate::{ScalarUnaryRdfOp, ThinResult};
use md5::Digest;
use model::{OwnedStringLiteral, SimpleLiteralRef};
use sha2::Sha384;

#[derive(Debug)]
pub struct Sha384RdfOp;

impl Default for Sha384RdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha384RdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for Sha384RdfOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Sha384::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
