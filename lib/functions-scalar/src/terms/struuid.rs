use crate::{ScalarNullaryRdfOp, ThinResult};
use datamodel::OwnedStringLiteral;
use uuid::Uuid;

#[derive(Debug)]
pub struct StrUuidRdfOp;

impl Default for StrUuidRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrUuidRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarNullaryRdfOp for StrUuidRdfOp {
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self) -> ThinResult<Self::Result<'data>> {
        let result = Uuid::new_v4().to_string();
        Ok(OwnedStringLiteral(result, None))
    }
}
