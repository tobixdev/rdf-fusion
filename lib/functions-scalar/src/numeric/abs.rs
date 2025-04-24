use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::Numeric;

#[derive(Debug)]
pub struct AbsRdfOp;

impl Default for AbsRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AbsRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AbsRdfOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        match value {
            Numeric::Int(value) => value.checked_abs().map(Numeric::Int),
            Numeric::Integer(value) => Ok(Numeric::Integer(value.checked_abs()?)),
            Numeric::Float(value) => Ok(Numeric::Float(value.abs())),
            Numeric::Double(value) => Ok(Numeric::Double(value.abs())),
            Numeric::Decimal(value) => value.checked_abs().map(Numeric::Decimal),
        }
    }
}
