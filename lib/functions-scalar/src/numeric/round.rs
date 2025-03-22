use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::Numeric;

#[derive(Debug)]
pub struct RoundRdfOp {}

impl RoundRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for RoundRdfOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        match value {
            Numeric::Float(value) => Ok(Numeric::Float(value.round())),
            Numeric::Double(value) => Ok(Numeric::Double(value.round())),
            Numeric::Decimal(value) => value.checked_round().map(Numeric::Decimal).ok_or(()),
            value => Ok(value),
        }
    }
}
