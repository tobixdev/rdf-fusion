use crate::{BinaryTermValueOp, SparqlOp, ThinResult};
use graphfusion_model::{Numeric, NumericPair};

#[derive(Debug)]
pub struct AddSparqlOp;

impl Default for AddSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AddSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AddSparqlOp {
    fn name(&self) -> &str {
        "add"
    }
}

impl BinaryTermValueOp for AddSparqlOp {
    type ArgLhs<'lhs> = Numeric;
    type ArgRhs<'rhs> = Numeric;
    type Result<'data> = Numeric;

    fn evaluate<'data>(
        &self,
        lhs: Self::ArgLhs<'data>,
        rhs: Self::ArgRhs<'data>,
    ) -> ThinResult<Self::Result<'data>> {
        match NumericPair::with_casts_from(lhs, rhs) {
            NumericPair::Int(lhs, rhs) => lhs.checked_add(rhs).map(Numeric::Int),
            NumericPair::Integer(lhs, rhs) => lhs.checked_add(rhs).map(Numeric::Integer),
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs + rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs + rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_add(rhs).map(Numeric::Decimal),
        }
    }
}
