use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::Boolean;
use graphfusion_model::TypedValueRef;

#[derive(Debug)]
pub struct IsBlankSparqlOp;

impl Default for IsBlankSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsBlankSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for IsBlankSparqlOp {}

impl UnarySparqlOp for IsBlankSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TypedValueRef::BlankNode(_));
        Ok(result.into())
    }
}
