use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use graphfusion_model::OwnedStringLiteral;
use graphfusion_model::TypedValueRef;

#[derive(Debug)]
pub struct StrSparqlOp;

impl Default for StrSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrSparqlOp {
}

impl UnarySparqlOp for StrSparqlOp {
    type Arg<'data> = TypedValueRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            TypedValueRef::NamedNode(value) => value.as_str().to_owned(),
            TypedValueRef::BlankNode(value) => value.as_str().to_owned(),
            TypedValueRef::BooleanLiteral(value) => value.to_string(),
            TypedValueRef::NumericLiteral(value) => value.format_value(),
            TypedValueRef::SimpleLiteral(value) => value.value.to_owned(),
            TypedValueRef::LanguageStringLiteral(value) => value.value.to_owned(),
            TypedValueRef::DateTimeLiteral(value) => value.to_string(),
            TypedValueRef::TimeLiteral(value) => value.to_string(),
            TypedValueRef::DateLiteral(value) => value.to_string(),
            TypedValueRef::DurationLiteral(value) => value.to_string(),
            TypedValueRef::YearMonthDurationLiteral(value) => value.to_string(),
            TypedValueRef::DayTimeDurationLiteral(value) => value.to_string(),
            TypedValueRef::OtherLiteral(value) => value.value().to_owned(),
        };
        Ok(OwnedStringLiteral(result, None))
    }
}
