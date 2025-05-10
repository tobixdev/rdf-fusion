use crate::{NullarySparqlOp, SparqlOp, ThinResult};
use graphfusion_model::OwnedStringLiteral;
use uuid::Uuid;

#[derive(Debug)]
pub struct StrUuidSparqlOp;

impl Default for StrUuidSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrUuidSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrUuidSparqlOp {
}

impl NullarySparqlOp for StrUuidSparqlOp {
    type Result = OwnedStringLiteral;

    fn evaluate(&self) -> ThinResult<Self::Result> {
        let result = Uuid::new_v4().to_string();
        Ok(OwnedStringLiteral(result, None))
    }
}
