use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::Numeric;

#[derive(Debug)]
pub struct AbsRdfOp {}

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
            Numeric::Int(value) => value.checked_abs().map(Numeric::Int).ok_or(()),
            Numeric::Integer(value) => Ok(Numeric::Integer(value)),
            Numeric::Float(value) => Ok(Numeric::Float(value.abs())),
            Numeric::Double(value) => Ok(Numeric::Double(value.abs())),
            Numeric::Decimal(value) => value.checked_abs().map(Numeric::Decimal).ok_or(()),
        }
    }
}
