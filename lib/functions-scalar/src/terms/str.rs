use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use graphfusion_model::{OwnedStringLiteral, TermRef};
use graphfusion_model::{TermValueRef, SimpleLiteralRef};

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
    fn name(&self) -> &str {
        "str"
    }
}

impl UnaryTermValueOp for StrSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            TermValueRef::NamedNode(value) => value.as_str().to_owned(),
            TermValueRef::BlankNode(value) => value.as_str().to_owned(),
            TermValueRef::BooleanLiteral(value) => value.to_string(),
            TermValueRef::NumericLiteral(value) => value.format_value(),
            TermValueRef::SimpleLiteral(value) => value.value.to_owned(),
            TermValueRef::LanguageStringLiteral(value) => value.value.to_owned(),
            TermValueRef::DateTimeLiteral(value) => value.to_string(),
            TermValueRef::TimeLiteral(value) => value.to_string(),
            TermValueRef::DateLiteral(value) => value.to_string(),
            TermValueRef::DurationLiteral(value) => value.to_string(),
            TermValueRef::YearMonthDurationLiteral(value) => value.to_string(),
            TermValueRef::DayTimeDurationLiteral(value) => value.to_string(),
            TermValueRef::OtherLiteral(value) => value.value().to_owned(),
        };
        Ok(OwnedStringLiteral(result, None))
    }
}

impl UnaryRdfTermOp for StrSparqlOp {
    type Result<'data> = SimpleLiteralRef<'data>;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        let result = match value {
            TermRef::NamedNode(value) => value.as_str(),
            TermRef::BlankNode(value) => value.as_str(),
            TermRef::Literal(value) => value.value(),
        };
        Ok(SimpleLiteralRef::new(result))
    }
}
