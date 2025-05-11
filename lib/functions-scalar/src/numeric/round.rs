use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::Numeric;

#[derive(Debug)]
pub struct RoundSparqlOp;

impl Default for RoundSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for RoundSparqlOp {}

impl UnarySparqlOp for RoundSparqlOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            Numeric::Float(value) => Ok(Numeric::Float(value.round())),
            Numeric::Double(value) => Ok(Numeric::Double(value.round())),
            Numeric::Decimal(value) => value.checked_round().map(Numeric::Decimal),
            _ => Ok(value),
        }
    }
}
