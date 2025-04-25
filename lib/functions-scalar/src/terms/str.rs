use crate::{ScalarUnaryRdfOp, ThinResult};
use model::{OwnedStringLiteral, InternalTermRef};

#[derive(Debug)]
pub struct StrRdfOp;

impl Default for StrRdfOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrRdfOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl ScalarUnaryRdfOp for StrRdfOp {
    type Arg<'data> = InternalTermRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            InternalTermRef::NamedNode(value) => value.as_str().to_owned(),
            InternalTermRef::BlankNode(value) => value.as_str().to_owned(),
            InternalTermRef::BooleanLiteral(value) => value.to_string(),
            InternalTermRef::NumericLiteral(value) => value.format_value(),
            InternalTermRef::SimpleLiteral(value) => value.value.to_owned(),
            InternalTermRef::LanguageStringLiteral(value) => value.value.to_owned(),
            InternalTermRef::DateTimeLiteral(value) => value.to_string(),
            InternalTermRef::TimeLiteral(value) => value.to_string(),
            InternalTermRef::DateLiteral(value) => value.to_string(),
            InternalTermRef::DurationLiteral(value) => value.to_string(),
            InternalTermRef::YearMonthDurationLiteral(value) => value.to_string(),
            InternalTermRef::DayTimeDurationLiteral(value) => value.to_string(),
            InternalTermRef::TypedLiteral(value) => value.value.to_owned(),
        };
        Ok(OwnedStringLiteral(result, None))
    }
}
