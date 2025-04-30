use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{InternalTermRef, SimpleLiteralRef, ThinError};

#[derive(Debug)]
pub struct LangRdfOp;

impl Default for LangRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl LangRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for LangRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = SimpleLiteralRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            InternalTermRef::NamedNode(_) | InternalTermRef::BlankNode(_) => {
                return ThinError::expected()
            }
            InternalTermRef::LanguageStringLiteral(value) => value.language,
            _ => "",
        };
        Ok(Self::Result::new(result))
    }
}
