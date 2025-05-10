use crate::{NullarySparqlOp, SparqlOp, ThinResult};
use graphfusion_model::Double;
use rand::random;

#[derive(Debug)]
pub struct RandSparqlOp;

impl Default for RandSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RandSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for RandSparqlOp {
}

impl NullarySparqlOp for RandSparqlOp {
    type Result = Double;

    fn evaluate(&self) -> ThinResult<Self::Result> {
        Ok(random::<f64>().into())
    }
}
