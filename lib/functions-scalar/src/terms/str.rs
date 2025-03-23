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
            TermRef::BooleanLiteral(value) => value.to_string(),
            TermRef::NumericLiteral(value) => value.format_value(),
            TermRef::SimpleLiteral(value) => value.value.to_string(),
            TermRef::LanguageStringLiteral(value) => value.value.to_string(),
            TermRef::DateTimeLiteral(value) => value.to_string(),
            TermRef::TimeLiteral(value) =>  value.to_string(),
            TermRef::DateLiteral(value) => value.to_string(),
            TermRef::DurationLiteral(value) => value.to_string(),
            TermRef::YearMonthDurationLiteral(value) => value.to_string(),
            TermRef::DayTimeDurationLiteral(value) => value.to_string(),
            TermRef::TypedLiteral(value) => value.value.to_string(),
        };
        Ok(OwnedStringLiteral(result, None))
    }
}
