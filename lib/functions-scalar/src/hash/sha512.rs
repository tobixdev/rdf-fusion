use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::{OwnedStringLiteral, SimpleLiteralRef};
use md5::Digest;
use sha2::Sha512;

#[derive(Debug)]
pub struct Sha512SparqlOp;

impl Default for Sha512SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Sha512SparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for Sha512SparqlOp {}

impl UnarySparqlOp for Sha512SparqlOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Sha512::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
