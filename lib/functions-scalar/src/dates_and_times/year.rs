use crate::{SparqlOp, ThinResult, UnaryTermValueOp};
use graphfusion_model::{DateTime, Integer};

#[derive(Debug)]
pub struct YearSparqlOp;

impl Default for YearSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl YearSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for YearSparqlOp {
    fn name(&self) -> &str {
        "year"
    }
}

impl UnaryTermValueOp for YearSparqlOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.year().into())
    }
}
