use crate::{UnaryTermValueOp, ThinResult, SparqlOp};
use graphfusion_model::Numeric;
use std::ops::Neg;

#[derive(Debug)]
pub struct UnaryMinusSparqlOp;

impl Default for UnaryMinusSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UnaryMinusSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for UnaryMinusSparqlOp {
    fn name(&self) -> &str {
        "minus"
    }
}

impl UnaryTermValueOp for UnaryMinusSparqlOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        match value {
            Numeric::Int(value) => value.checked_neg().map(Numeric::Int),
            Numeric::Integer(value) => value.checked_neg().map(Numeric::Integer),
            Numeric::Float(value) => Ok(Numeric::Float(value.neg())),
            Numeric::Double(value) => Ok(Numeric::Double(value.neg())),
            Numeric::Decimal(value) => value.checked_neg().map(Numeric::Decimal),
        }
    }
}
