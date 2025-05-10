use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::TypedValueRef;
use graphfusion_model::Boolean;

#[derive(Debug)]
pub struct IsIriSparqlOp;

impl Default for IsIriSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsIriSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for IsIriSparqlOp {
}

impl UnarySparqlOp for IsIriSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TypedValueRef::NamedNode(_));
        Ok(result.into())
    }
}