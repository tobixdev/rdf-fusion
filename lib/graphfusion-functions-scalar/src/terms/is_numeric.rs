use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use model::TermValueRef;
use model::{is_numeric_datatype, Boolean, TermRef};

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

impl SparqlOp for IsNumericSparqlOp {
    fn name(&self) -> &str {
        "is_numeric"
    }
}

impl UnaryTermValueOp for IsNumericSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        // Datatypes that are not directly supported should have been promoted.
        let result = matches!(value, TermValueRef::NumericLiteral(_));
        Ok(result.into())
    }
}

impl UnaryRdfTermOp for IsNumericSparqlOp {
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            TermRef::Literal(lit) => is_numeric_datatype(lit.datatype()),
            _ => false
        };
        Ok(result.into())
    }
}
