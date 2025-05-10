use crate::{UnarySparqlOp, ThinResult, SparqlOp};
use graphfusion_model::Numeric;

#[derive(Debug)]
pub struct CeilSparqlOp;

impl Default for CeilSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CeilSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for CeilSparqlOp {
}

impl UnarySparqlOp for CeilSparqlOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            Numeric::Float(value) => Ok(Numeric::Float(value.ceil())),
            Numeric::Double(value) => Ok(Numeric::Double(value.ceil())),
            Numeric::Decimal(value) => value.checked_ceil().map(Numeric::Decimal),
            _ => Ok(value),
        }
    }
}
