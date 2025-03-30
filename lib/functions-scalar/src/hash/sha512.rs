use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{OwnedStringLiteral, SimpleLiteralRef};
use md5::Digest;
use sha2::Sha512;

#[derive(Debug)]
pub struct Sha512RdfOp {}

impl Sha512RdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for Sha512RdfOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let hash = hex::encode(Sha512::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
