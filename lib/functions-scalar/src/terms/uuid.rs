use crate::{RdfOpResult, ScalarNullaryRdfOp};
use oxrdf::NamedNode;
use uuid::Uuid;

#[derive(Debug)]
pub struct UuidRdfOp {}

impl UuidRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNullaryRdfOp for UuidRdfOp {
    type Result<'data> = NamedNode;

    fn evaluate<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        let formatted = format!("urn:uuid:{}", Uuid::new_v4());
        Ok(NamedNode::new_unchecked(formatted))
    }
}
