use crate::{UnaryTermValueOp, ThinResult, SparqlOp};
use md5::Digest;
use graphfusion_model::{OwnedStringLiteral, SimpleLiteralRef};
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

impl SparqlOp for Sha256SparqlOp {
    fn name(&self) -> &str {
        "sha256"
    }
}

impl UnaryTermValueOp for Sha256SparqlOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Sha256::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
