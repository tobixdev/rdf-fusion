use crate::{ScalarNullaryRdfOp, ThinResult};
use oxrdf::NamedNode;
use uuid::Uuid;

#[derive(Debug)]
pub struct UuidRdfOp;

impl Default for UuidRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl UuidRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNullaryRdfOp for UuidRdfOp {
    type Result<'data> = NamedNode;

    fn evaluate<'data>(&self) -> ThinResult<Self::Result<'data>> {
        let formatted = format!("urn:uuid:{}", Uuid::new_v4());
        Ok(NamedNode::new_unchecked(formatted))
    }
}
