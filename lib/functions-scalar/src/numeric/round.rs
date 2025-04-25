use crate::{ScalarUnaryRdfOp, ThinResult};
use model::Numeric;

#[derive(Debug)]
pub struct RoundRdfOp;

impl Default for RoundRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RoundRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for RoundRdfOp {
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
