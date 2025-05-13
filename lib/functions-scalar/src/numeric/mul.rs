use crate::{BinarySparqlOp, SparqlOp, ThinResult};
use rdf_fusion_model::{Numeric, NumericPair};

#[derive(Debug)]
pub struct MulSparqlOp;

impl Default for MulSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl MulSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for MulSparqlOp {}

impl BinarySparqlOp for MulSparqlOp {
    type ArgLhs<'lhs> = Numeric;
    type ArgRhs<'rhs> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        match NumericPair::with_casts_from(lhs, rhs) {
            NumericPair::Int(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Int),
            NumericPair::Integer(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Integer),
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs * rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs * rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Decimal),
        }
    }
}
