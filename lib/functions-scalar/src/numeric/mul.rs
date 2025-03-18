use crate::{RdfOpResult, ScalarBinaryRdfOp};
use datamodel::{Numeric, NumericPair};

#[derive(Debug)]
pub struct MulRdfOp {}

impl MulRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarBinaryRdfOp for MulRdfOp {
    type ArgLhs<'lhs> = Numeric;
    type ArgRhs<'rhs> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> RdfOpResult<Self::Result<'data>> {
        match NumericPair::with_casts_from(lhs, rhs) {
            NumericPair::Int(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Int).ok_or(()),
            NumericPair::Integer(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Integer).ok_or(()),
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs * rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs * rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Decimal).ok_or(()),
        }
    }
}
