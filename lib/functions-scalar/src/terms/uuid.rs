use crate::{NullarySparqlOp, SparqlOp, ThinResult};
use graphfusion_model::NamedNode;
use uuid::Uuid;

#[derive(Debug)]
pub struct UuidSparqlOp;

impl Default for UuidSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UuidSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for UuidSparqlOp {}

impl NullarySparqlOp for UuidSparqlOp {
    type Result = NamedNode;

    fn evaluate(&self) -> ThinResult<Self::Result> {
        let formatted = format!("urn:uuid:{}", Uuid::new_v4());
        Ok(NamedNode::new_unchecked(formatted))
    }
}
