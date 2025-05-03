use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use model::TermValueRef;
use model::{Boolean, TermRef};

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

impl SparqlOp for IsLiteralSparqlOp {
    fn name(&self) -> &str {
        "is_literal"
    }
}

impl UnaryTermValueOp for IsLiteralSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = !matches!(value, TermValueRef::NamedNode(_) | TermValueRef::BlankNode(_));
        Ok(result.into())
    }
}

impl UnaryRdfTermOp for IsLiteralSparqlOp {
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TermRef::Literal(_));
        Ok(result.into())
    }
}
