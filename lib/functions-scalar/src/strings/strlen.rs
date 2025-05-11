use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::{Integer, StringLiteralRef};

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

impl SparqlOp for StrLenSparqlOp {}

impl UnarySparqlOp for StrLenSparqlOp {
    type Arg<'data> = StringLiteralRef<'data>;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let value: i64 = value.len().try_into()?;
        Ok(value.into())
    }
}
