use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::{DateTime, Decimal};

#[derive(Debug)]
pub struct SecondsSparqlOp;

impl Default for SecondsSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SecondsSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for SecondsSparqlOp {}

impl UnarySparqlOp for SecondsSparqlOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Decimal;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.second())
    }
}
