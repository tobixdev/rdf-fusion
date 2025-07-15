use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::Boolean;
use rdf_fusion_model::TypedValueRef;

/// TODO #20 Remove
#[derive(Debug)]
pub struct IsIriLegacySparqlOp;

impl Default for IsIriLegacySparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsIriLegacySparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for IsIriLegacySparqlOp {}

impl UnarySparqlOp for IsIriLegacySparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TypedValueRef::NamedNode(_));
        Ok(result.into())
    }
}
