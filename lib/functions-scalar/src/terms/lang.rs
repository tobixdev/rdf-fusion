use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::TypedValueRef;
use graphfusion_model::{SimpleLiteralRef, ThinError};

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
}

impl UnarySparqlOp for LangSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = SimpleLiteralRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            TypedValueRef::NamedNode(_) | TypedValueRef::BlankNode(_) => {
                return ThinError::expected()
            }
            TypedValueRef::LanguageStringLiteral(value) => value.language,
            _ => "",
        };
        Ok(Self::Result::new(result))
    }
}
