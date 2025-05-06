use crate::{SparqlOp, ThinResult, UnaryRdfTermOp, UnaryTermValueOp};
use graphfusion_model::TermValueRef;
use graphfusion_model::{OwnedStringLiteral, SimpleLiteralRef, TermRef, ThinError};

#[derive(Debug)]
pub struct AsStringSparqlOp;

impl Default for AsStringSparqlOp {
    fn default() -> Self {
        Self::new()
    }
}

impl AsStringSparqlOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for AsStringSparqlOp {
    fn name(&self) -> &str {
        "xsd:string"
    }
}

impl UnaryTermValueOp for AsStringSparqlOp {
    type Arg<'data> = TermValueRef<'data>;
    type Result<'data> = OwnedStringLiteral;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        let converted = match value {
            TermValueRef::NamedNode(value) => value.as_str().to_owned(),
            TermValueRef::BlankNode(_) => return ThinError::expected(),
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
        Ok(OwnedStringLiteral(converted, None))
    }
}

impl UnaryRdfTermOp for AsStringSparqlOp {
    type Result<'data> = SimpleLiteralRef<'data>;

    fn evaluate<'data>(&self, value: TermRef<'data>) -> ThinResult<Self::Result<'data>> {
        let value = match value {
            TermRef::NamedNode(value) => value.as_str(),
            TermRef::BlankNode(value) => value.as_str(),
            TermRef::Literal(literal) => literal.value(),
        };
        Ok(SimpleLiteralRef::new(value))
    }
}
