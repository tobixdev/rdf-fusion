use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{OwnedStringLiteral, SimpleLiteralRef};
use md5::Digest;
use sha2::Sha256;

#[derive(Debug)]
pub struct Sha256RdfOp;

impl Default for Sha256RdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256RdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for Sha256RdfOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let hash = hex::encode(Sha256::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
