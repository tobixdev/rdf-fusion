use crate::{UnaryTermValueOp, ThinResult, SparqlOp};
use md5::Digest;
use model::{OwnedStringLiteral, SimpleLiteralRef};
use sha2::Sha384;

#[derive(Debug)]
pub struct Sha384SparqlOp;

impl Default for Sha384SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha384SparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for Sha384SparqlOp {
    fn name(&self) -> &str {
        "sha384"
    }
}

impl UnaryTermValueOp for Sha384SparqlOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Sha384::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
