use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use model::{DateTime, Integer};

#[derive(Debug)]
pub struct DaySparqlOp;

impl Default for DaySparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl DaySparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for DaySparqlOp {
    fn name(&self) -> &str {
        "day"
    }
}

impl UnaryTermValueOp for DaySparqlOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.day().into())
    }
}
