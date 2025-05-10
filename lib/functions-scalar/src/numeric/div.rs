use crate::{BinarySparqlOp, SparqlOp};
use graphfusion_model::{Decimal, Numeric, NumericPair, ThinResult};

#[derive(Debug)]
pub struct DivSparqlOp;

impl Default for DivSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl DivSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for DivSparqlOp {
}

impl BinarySparqlOp for DivSparqlOp {
    type ArgLhs<'lhs> = Numeric;
    type ArgRhs<'rhs> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        match NumericPair::with_casts_from(lhs, rhs) {
            NumericPair::Int(lhs, rhs) => Decimal::from(lhs).checked_div(rhs).map(Numeric::Decimal),
            NumericPair::Integer(lhs, rhs) => {
                Decimal::from(lhs).checked_div(rhs).map(Numeric::Decimal)
            }
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs / rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs / rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_div(rhs).map(Numeric::Decimal),
        }
    }
}
