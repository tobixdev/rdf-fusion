use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::Numeric;

#[derive(Debug)]
pub struct FloorRdfOp;

impl Default for FloorRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl FloorRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for FloorRdfOp {
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
