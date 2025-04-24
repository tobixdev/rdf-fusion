use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{OwnedStringLiteral, SimpleLiteralRef};
use md5::Digest;
use sha1::Sha1;

#[derive(Debug)]
pub struct Sha1RdfOp;

impl Default for Sha1RdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha1RdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for Sha1RdfOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let hash = hex::encode(Sha1::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
