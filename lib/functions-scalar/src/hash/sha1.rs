use crate::{UnarySparqlOp, ThinResult, SparqlOp};
use md5::Digest;
use graphfusion_model::{OwnedStringLiteral, SimpleLiteralRef};
use sha1::Sha1;

#[derive(Debug)]
pub struct Sha1SparqlOp;

impl Default for Sha1SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha1SparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for Sha1SparqlOp {
}

impl UnarySparqlOp for Sha1SparqlOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Sha1::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
