use crate::{UnaryTermValueOp, ThinResult, SparqlOp};
use graphfusion_model::{DateTime, Integer};

#[derive(Debug)]
pub struct MonthSparqlOp;

impl Default for MonthSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl MonthSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for MonthSparqlOp {
    fn name(&self) -> &str {
        "month"
    }
}

impl UnaryTermValueOp for MonthSparqlOp {
    type Arg<'data> = DateTime;
    type Result<'data> = Integer;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(value.month().into())
    }
}
