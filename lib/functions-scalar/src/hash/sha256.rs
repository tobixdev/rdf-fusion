use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::{OwnedStringLiteral, SimpleLiteralRef};
use md5::Digest;
use sha2::Sha256;

#[derive(Debug)]
pub struct Sha256SparqlOp;

impl Default for Sha256SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha256SparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for Sha256SparqlOp {}

impl UnarySparqlOp for Sha256SparqlOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Sha256::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
