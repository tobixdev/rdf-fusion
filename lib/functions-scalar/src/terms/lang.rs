use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{TermRef, SimpleLiteralRef};

#[derive(Debug)]
pub struct LangRdfOp {}

impl LangRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for LangRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = SimpleLiteralRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let result = match value {
            TermRef::NamedNode(_) | TermRef::BlankNode(_) => return Err(()),
            TermRef::LanguageStringLiteral(value) => value.language,
            _ => "",
        };
        Ok(Self::Result::new(result))
    }
}
