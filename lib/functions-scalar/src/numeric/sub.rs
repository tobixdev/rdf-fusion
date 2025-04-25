use crate::{ScalarBinaryRdfOp, ThinResult};
use datamodel::{Numeric, NumericPair};

#[derive(Debug)]
pub struct SubRdfOp;

impl Default for SubRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl SubRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for SubRdfOp {
    type ArgLhs<'lhs> = Numeric;
    type ArgRhs<'rhs> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        match NumericPair::with_casts_from(lhs, rhs) {
            NumericPair::Int(lhs, rhs) => lhs.checked_sub(rhs).map(Numeric::Int),
            NumericPair::Integer(lhs, rhs) => lhs.checked_sub(rhs).map(Numeric::Integer),
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs - rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs - rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_sub(rhs).map(Numeric::Decimal),
        }
    }
}
