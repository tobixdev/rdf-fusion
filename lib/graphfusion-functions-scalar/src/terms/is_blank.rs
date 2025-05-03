use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use model::TermValueRef;
use model::{Boolean, TermRef};

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

impl SparqlOp for IsBlankSparqlOp {
    fn name(&self) -> &str {
        "is_blank"
    }
}

impl UnaryTermValueOp for IsBlankSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TermValueRef::BlankNode(_));
        Ok(result.into())
    }
}

impl UnaryRdfTermOp for IsBlankSparqlOp {
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TermRef::BlankNode(_));
        Ok(result.into())
    }
}
