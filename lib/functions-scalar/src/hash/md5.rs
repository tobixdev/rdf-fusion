use crate::{UnaryTermValueOp, ThinResult, SparqlOp};
use md5::{Digest, Md5};
use graphfusion_model::{OwnedStringLiteral, SimpleLiteralRef};

#[derive(Debug)]
pub struct Md5SparqlOp;

impl Default for Md5SparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl Md5SparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for Md5SparqlOp {
    fn name(&self) -> &str {
        "md5"
    }
}

impl UnaryTermValueOp for Md5SparqlOp {
    type Arg<'data> = SimpleLiteralRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let hash = hex::encode(Md5::new().chain_update(value.value).finalize());
        Ok(OwnedStringLiteral::new(hash, None))
    }
}
