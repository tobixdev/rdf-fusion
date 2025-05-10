use crate::{UnarySparqlOp, ThinResult, SparqlOp};
use graphfusion_model::Numeric;

#[derive(Debug)]
pub struct AbsSparqlOp;

impl Default for AbsSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AbsSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AbsSparqlOp {
}

impl UnarySparqlOp for AbsSparqlOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            Numeric::Int(value) => value.checked_abs().map(Numeric::Int),
            Numeric::Integer(value) => Ok(Numeric::Integer(value.checked_abs()?)),
            Numeric::Float(value) => Ok(Numeric::Float(value.abs())),
            Numeric::Double(value) => Ok(Numeric::Double(value.abs())),
            Numeric::Decimal(value) => value.checked_abs().map(Numeric::Decimal),
        }
    }
}
