use crate::{RdfOpResult, ScalarNullaryRdfOp};
use datamodel::Double;
use rand::random;

#[derive(Debug)]
pub struct RandRdfOp {}

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
