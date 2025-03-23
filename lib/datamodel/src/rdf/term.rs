use crate::{
    Boolean, Date, DateTime, DayTimeDuration, DecodedTerm, Duration, LanguageStringRef, Numeric,
    RdfOpResult, RdfValueRef, SimpleLiteralRef, Time, TypedLiteralRef, YearMonthDuration,
};
use oxrdf::vocab::xsd;
use oxrdf::{BlankNodeRef, Literal, NamedNodeRef};

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum TermRef<'value> {
    NamedNode(NamedNodeRef<'value>),
    BlankNode(BlankNodeRef<'value>),
    BooleanLiteral(Boolean),
    NumericLiteral(Numeric),
    SimpleLiteral(SimpleLiteralRef<'value>),
    LanguageStringLiteral(LanguageStringRef<'value>),
    DateTimeLiteral(DateTime),
    TimeLiteral(Time),
    DateLiteral(Date),
    DurationLiteral(Duration),
    YearMonthDurationLiteral(YearMonthDuration),
    DayTimeDurationLiteral(DayTimeDuration),
    TypedLiteral(TypedLiteralRef<'value>),
}

impl TermRef<'_> {
    /// Returns an owned decoded term.
    pub fn into_decoded(self) -> DecodedTerm {
        match self {
            TermRef::NamedNode(value) => DecodedTerm::NamedNode(value.into_owned()),
            TermRef::BlankNode(value) => DecodedTerm::BlankNode(value.into_owned()),
            TermRef::BooleanLiteral(value) => DecodedTerm::Literal(Literal::from(value.as_bool())),
            TermRef::NumericLiteral(value) => match value {
                Numeric::Int(value) => DecodedTerm::Literal(Literal::from(i32::from(value))),
                Numeric::Integer(value) => DecodedTerm::Literal(Literal::from(i64::from(value))),
                Numeric::Float(value) => DecodedTerm::Literal(Literal::from(f32::from(value))),
                Numeric::Double(value) => DecodedTerm::Literal(Literal::from(f64::from(value))),
                Numeric::Decimal(value) => DecodedTerm::Literal(Literal::new_typed_literal(
                    value.to_string(),
                    xsd::DECIMAL,
                )),
            },
            TermRef::SimpleLiteral(value) => DecodedTerm::Literal(Literal::from(value.value)),
            TermRef::LanguageStringLiteral(value) => DecodedTerm::Literal(
                Literal::new_language_tagged_literal_unchecked(value.value, value.language),
            ),
            TermRef::DateTimeLiteral(value) => DecodedTerm::Literal(Literal::new_typed_literal(
                value.to_string(),
                xsd::DATE_TIME,
            )),
            TermRef::TimeLiteral(value) => {
                DecodedTerm::Literal(Literal::new_typed_literal(value.to_string(), xsd::TIME))
            }
            TermRef::DateLiteral(value) => {
                DecodedTerm::Literal(Literal::new_typed_literal(value.to_string(), xsd::DATE))
            }
            TermRef::DurationLiteral(value) => {
                DecodedTerm::Literal(Literal::new_typed_literal(value.to_string(), xsd::DURATION))
            }
            TermRef::YearMonthDurationLiteral(value) => DecodedTerm::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::YEAR_MONTH_DURATION),
            ),
            TermRef::DayTimeDurationLiteral(value) => DecodedTerm::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::DAY_TIME_DURATION),
            ),
            TermRef::TypedLiteral(value) => DecodedTerm::Literal(Literal::new_typed_literal(
                value.value,
                NamedNodeRef::new_unchecked(value.literal_type),
            )),
        }
    }
}

impl<'data> RdfValueRef<'data> for TermRef<'data> {
    fn from_term(term: TermRef<'data>) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        Ok(term)
    }
}
