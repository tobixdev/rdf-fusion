use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use model::{Integer, StringLiteralRef};

#[derive(Debug)]
pub struct StrLenSparqlOp;

impl Default for StrLenSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrLenSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrLenSparqlOp {
    fn name(&self) -> &str {
        "strlen"
    }
}

impl UnaryTermValueOp for StrLenSparqlOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let value: i64 = value.len().try_into()?;
        Ok(value.into())
    }
}
