use crate::{SparqlOp, ThinResult, UnarySparqlOp};
use rdf_fusion_model::TypedValueRef;
use rdf_fusion_model::{OwnedStringLiteral, SimpleLiteralRef, TermRef};

#[derive(Debug)]
pub struct StrTypedValueOp;

impl Default for StrTypedValueOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrTypedValueOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrTypedValueOp {}

impl UnarySparqlOp for StrTypedValueOp {
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

#[derive(Debug)]
pub struct StrTermOp;

impl Default for StrTermOp {
    fn default() -> Self {
        Self::new()
    }
}

impl StrTermOp {
    pub fn new() -> Self {
        Self {}
    }
}

impl SparqlOp for StrTermOp {}

impl UnarySparqlOp for StrTermOp {
    type Arg<'data> = TermRef<'data>;
    type Result<'data> = SimpleLiteralRef<'data>;

    fn evaluate<'data>(&self, value: Self::Arg<'data>) -> ThinResult<Self::Result<'data>> {
        Ok(match value {
            TermRef::NamedNode(v) => SimpleLiteralRef::new(v.as_str()),
            TermRef::BlankNode(v) => SimpleLiteralRef::new(v.as_str()),
            TermRef::Literal(v) => SimpleLiteralRef::new(v.value()),
        })
    }
}
