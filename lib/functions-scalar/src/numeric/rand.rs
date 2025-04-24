use crate::{RdfOpResult, ScalarNullaryRdfOp};
use datamodel::Double;
use rand::random;

#[derive(Debug)]
pub struct RandRdfOp;

impl Default for RandRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl RandRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNullaryRdfOp for RandRdfOp {
    type Result<'data> = Double;

    fn evaluate<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        Ok(random::<f64>().into())
    }
}
