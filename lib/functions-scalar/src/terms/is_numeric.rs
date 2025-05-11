use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::Boolean;
use graphfusion_model::TypedValueRef;

#[derive(Debug)]
pub struct IsNumericSparqlOp;

impl Default for IsNumericSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNumericSparqlOp {
    pub fn new() -> Self {
        Self
    }
}

impl SparqlOp for IsNumericSparqlOp {}

impl UnarySparqlOp for IsNumericSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        // Datatypes that are not directly supported should have been promoted.
        let result = matches!(value, TypedValueRef::NumericLiteral(_));
        Ok(result.into())
    }
}
