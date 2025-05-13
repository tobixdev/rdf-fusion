use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::TypedValueRef;
use rdf_fusion_model::{DateTime, ThinError};

#[derive(Debug)]
pub struct AsDateTimeSparqlOp;

impl Default for AsDateTimeSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsDateTimeSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsDateTimeSparqlOp {}

impl UnarySparqlOp for AsDateTimeSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = DateTime;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TypedValueRef::SimpleLiteral(v) => v.value.parse()?,
            TypedValueRef::DateTimeLiteral(v) => v,
            _ => return ThinError::expected(),
        };
        Ok(converted)
    }
}
