use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::Numeric;

#[derive(Debug)]
pub struct FloorSparqlOp;

impl Default for FloorSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl FloorSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for FloorSparqlOp {}

impl UnarySparqlOp for FloorSparqlOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            Numeric::Float(value) => Ok(Numeric::Float(value.floor())),
            Numeric::Double(value) => Ok(Numeric::Double(value.floor())),
            Numeric::Decimal(value) => value.checked_floor().map(Numeric::Decimal),
            _ => Ok(value),
        }
    }
}
