use crate::{RdfOpResult, ScalarNullaryRdfOp};
use datamodel::OwnedStringLiteral;
use uuid::Uuid;

#[derive(Debug)]
pub struct UuidRdfOp {
}

impl UuidRdfOp {
    pub fn new() -> Self {
        Self {
        }
    }
}

impl ScalarNullaryRdfOp for UuidRdfOp {
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self) -> RdfOpResult<Self::Result<'data>> {
        let formatted = format!("urn:uuid:{}", Uuid::new_v4());
        Ok(OwnedStringLiteral(formatted, None))
    }
}