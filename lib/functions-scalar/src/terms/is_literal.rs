use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::Boolean;
use rdf_fusion_model::TypedValueRef;

#[derive(Debug)]
pub struct IsLiteralSparqlOp;

impl Default for IsLiteralSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsLiteralSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for IsLiteralSparqlOp {}

impl UnarySparqlOp for IsLiteralSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = !matches!(
            value,
            TypedValueRef::NamedNode(_) | TypedValueRef::BlankNode(_)
        );
        Ok(result.into())
    }
}
