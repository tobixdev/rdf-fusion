use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use graphfusion_model::TermValueRef;
use graphfusion_model::{SimpleLiteralRef, TermRef, ThinError};

#[derive(Debug)]
pub struct LangSparqlOp;

impl Default for LangSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LangSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for LangSparqlOp {
    fn name(&self) -> &str {
        "lang"
    }
}

impl UnaryTermValueOp for LangSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = SimpleLiteralRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            TermValueRef::NamedNode(_) | TermValueRef::BlankNode(_) => {
                return ThinError::expected()
            }
            TermValueRef::LanguageStringLiteral(value) => value.language,
            _ => "",
        };
        Ok(Self::Result::new(result))
    }
}

impl UnaryRdfTermOp for LangSparqlOp {
    type Result<'data> = SimpleLiteralRef<'data>;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            TermRef::NamedNode(_) | TermRef::BlankNode(_) => return ThinError::expected(),
            TermRef::Literal(lit) => lit.language().unwrap_or(""),
        };
        Ok(Self::Result::new(result))
    }
}
