use crate::{RdfOpResult, ScalarUnaryRdfOp};
use datamodel::{OwnedStringLiteral, TermRef};

#[derive(Debug)]
pub struct StrRdfOp {}

impl StrRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for StrRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> RdfOpResult<Self::Result<'data>> {
        let result = match value {
            TermRef::NamedNode(value) => value.as_str().to_string(),
            TermRef::BlankNode(value) => value.as_str().to_string(),
            TermRef::Boolean(value) => value.to_string(),
            TermRef::Numeric(value) => value.format_value(),
            TermRef::SimpleLiteral(value) => value.value.to_string(),
            TermRef::LanguageString(value) => value.value.to_string(),
            TermRef::Duration(value) => value.to_string(),
            TermRef::TypedLiteral(value) => value.value.to_string(),
        };
        Ok(OwnedStringLiteral(result, None))
    }
}
