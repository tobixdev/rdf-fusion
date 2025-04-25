use crate::{ScalarUnaryRdfOp, ThinResult};
use datamodel::{OwnedStringLiteral, TermRef, ThinError};

#[derive(Debug)]
pub struct AsStringRdfOp;

impl Default for AsStringRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsStringRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for AsStringRdfOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermRef::NamedNode(value) => value.as_str().to_owned(),
            TermRef::BlankNode(_) => return ThinError::expected(),
            TermRef::BooleanLiteral(value) => value.to_string(),
            TermRef::NumericLiteral(value) => value.format_value(),
            TermRef::SimpleLiteral(value) => value.value.to_owned(),
            TermRef::LanguageStringLiteral(value) => value.value.to_owned(),
            TermRef::DateTimeLiteral(value) => value.to_string(),
            TermRef::TimeLiteral(value) => value.to_string(),
            TermRef::DateLiteral(value) => value.to_string(),
            TermRef::DurationLiteral(value) => value.to_string(),
            TermRef::YearMonthDurationLiteral(value) => value.to_string(),
            TermRef::DayTimeDurationLiteral(value) => value.to_string(),
            TermRef::TypedLiteral(value) => value.value.to_owned(),
        };
        Ok(OwnedStringLiteral(converted, None))
    }
}
