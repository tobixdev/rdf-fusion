use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::Numeric;

#[derive(Debug)]
pub struct CeilRdfOp;

impl Default for CeilRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl CeilRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for CeilRdfOp {
    type Arg<'data> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        match value {
            Numeric::Float(value) => Ok(Numeric::Float(value.ceil())),
            Numeric::Double(value) => Ok(Numeric::Double(value.ceil())),
            Numeric::Decimal(value) => value.checked_ceil().map(Numeric::Decimal),
            _ => Ok(value),
        }
    }
}
