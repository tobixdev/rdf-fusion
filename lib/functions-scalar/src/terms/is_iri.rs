use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use graphfusion_model::TermValueRef;
use graphfusion_model::{Boolean, TermRef};

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
    fn name(&self) -> &str {
        "is_iri"
    }
}

impl UnaryTermValueOp for IsIriSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TermValueRef::NamedNode(_));
        Ok(result.into())
    }
}

impl UnaryRdfTermOp for IsIriSparqlOp {
    type Result<'data> = Boolean;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        let result = matches!(value, TermRef::NamedNode(_));
        Ok(result.into())
    }
}
