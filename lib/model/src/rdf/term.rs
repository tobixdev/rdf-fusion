use crate::rdf::language_string::LanguageString;
use crate::rdf::simple_literal::SimpleLiteral;
use crate::rdf::typed_literal::TypedLiteral;
use crate::{
    Boolean, Date, DateTime, DayTimeDuration, DecodedTerm, Duration, LanguageStringRef, Numeric,
    RdfValueRef, SimpleLiteralRef, ThinResult, Time, TypedLiteralRef, YearMonthDuration,
};
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, BlankNodeRef, Literal, NamedNode, NamedNodeRef};
use std::cmp::Ordering;

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub enum InternalTerm {
    NamedNode(NamedNode),
    BlankNode(BlankNode),
    BooleanLiteral(Boolean),
    NumericLiteral(Numeric),
    SimpleLiteral(SimpleLiteral),
    LanguageStringLiteral(LanguageString),
    DateTimeLiteral(DateTime),
    TimeLiteral(Time),
    DateLiteral(Date),
    DurationLiteral(Duration),
    YearMonthDurationLiteral(YearMonthDuration),
    DayTimeDurationLiteral(DayTimeDuration),
    TypedLiteral(TypedLiteral),
}

impl InternalTerm {
    pub fn as_ref(&self) -> InternalTermRef<'_> {
        match self {
            InternalTerm::NamedNode(inner) => InternalTermRef::NamedNode(inner.as_ref()),
            InternalTerm::BlankNode(inner) => InternalTermRef::BlankNode(inner.as_ref()),
            InternalTerm::BooleanLiteral(inner) => InternalTermRef::BooleanLiteral(*inner),
            InternalTerm::NumericLiteral(inner) => InternalTermRef::NumericLiteral(*inner),
            InternalTerm::SimpleLiteral(inner) => InternalTermRef::SimpleLiteral(inner.as_ref()),
            InternalTerm::LanguageStringLiteral(inner) => InternalTermRef::LanguageStringLiteral(inner.as_ref()),
            InternalTerm::DateTimeLiteral(inner) => InternalTermRef::DateTimeLiteral(*inner),
            InternalTerm::TimeLiteral(inner) => InternalTermRef::TimeLiteral(*inner),
            InternalTerm::DateLiteral(inner) => InternalTermRef::DateLiteral(*inner),
            InternalTerm::DurationLiteral(inner) => InternalTermRef::DurationLiteral(*inner),
            InternalTerm::YearMonthDurationLiteral(inner) => InternalTermRef::YearMonthDurationLiteral(*inner),
            InternalTerm::DayTimeDurationLiteral(inner) => InternalTermRef::DayTimeDurationLiteral(*inner),
            InternalTerm::TypedLiteral(inner) => InternalTermRef::TypedLiteral(inner.as_ref()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum InternalTermRef<'value> {
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

impl InternalTermRef<'_> {
    /// Returns an owned decoded term.
    pub fn into_decoded(self) -> DecodedTerm {
        match self {
            InternalTermRef::NamedNode(value) => DecodedTerm::NamedNode(value.into_owned()),
            InternalTermRef::BlankNode(value) => DecodedTerm::BlankNode(value.into_owned()),
            InternalTermRef::BooleanLiteral(value) => DecodedTerm::Literal(Literal::from(value.as_bool())),
            InternalTermRef::NumericLiteral(value) => match value {
                Numeric::Int(value) => DecodedTerm::Literal(Literal::from(i32::from(value))),
                Numeric::Integer(value) => DecodedTerm::Literal(Literal::from(i64::from(value))),
                Numeric::Float(value) => DecodedTerm::Literal(Literal::from(f32::from(value))),
                Numeric::Double(value) => DecodedTerm::Literal(Literal::from(f64::from(value))),
                Numeric::Decimal(value) => DecodedTerm::Literal(Literal::new_typed_literal(
                    value.to_string(),
                    xsd::DECIMAL,
                )),
            },
            InternalTermRef::SimpleLiteral(value) => DecodedTerm::Literal(Literal::from(value.value)),
            InternalTermRef::LanguageStringLiteral(value) => DecodedTerm::Literal(
                Literal::new_language_tagged_literal_unchecked(value.value, value.language),
            ),
            InternalTermRef::DateTimeLiteral(value) => DecodedTerm::Literal(Literal::new_typed_literal(
                value.to_string(),
                xsd::DATE_TIME,
            )),
            InternalTermRef::TimeLiteral(value) => {
                DecodedTerm::Literal(Literal::new_typed_literal(value.to_string(), xsd::TIME))
            }
            InternalTermRef::DateLiteral(value) => {
                DecodedTerm::Literal(Literal::new_typed_literal(value.to_string(), xsd::DATE))
            }
            InternalTermRef::DurationLiteral(value) => {
                DecodedTerm::Literal(Literal::new_typed_literal(value.to_string(), xsd::DURATION))
            }
            InternalTermRef::YearMonthDurationLiteral(value) => DecodedTerm::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::YEAR_MONTH_DURATION),
            ),
            InternalTermRef::DayTimeDurationLiteral(value) => DecodedTerm::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::DAY_TIME_DURATION),
            ),
            InternalTermRef::TypedLiteral(value) => DecodedTerm::Literal(Literal::new_typed_literal(
                value.value,
                NamedNodeRef::new_unchecked(value.literal_type),
            )),
        }
    }

    pub fn into_owned(self) -> InternalTerm {
        match self {
            InternalTermRef::NamedNode(inner) => InternalTerm::NamedNode(inner.into_owned()),
            InternalTermRef::BlankNode(inner) => InternalTerm::BlankNode(inner.into_owned()),
            InternalTermRef::BooleanLiteral(inner) => InternalTerm::BooleanLiteral(inner),
            InternalTermRef::NumericLiteral(inner) => InternalTerm::NumericLiteral(inner),
            InternalTermRef::SimpleLiteral(inner) => InternalTerm::SimpleLiteral(inner.into_owned()),
            InternalTermRef::LanguageStringLiteral(inner) => {
                InternalTerm::LanguageStringLiteral(inner.into_owned())
            }
            InternalTermRef::DateTimeLiteral(inner) => InternalTerm::DateTimeLiteral(inner),
            InternalTermRef::TimeLiteral(inner) => InternalTerm::TimeLiteral(inner),
            InternalTermRef::DateLiteral(inner) => InternalTerm::DateLiteral(inner),
            InternalTermRef::DurationLiteral(inner) => InternalTerm::DurationLiteral(inner),
            InternalTermRef::YearMonthDurationLiteral(inner) => InternalTerm::YearMonthDurationLiteral(inner),
            InternalTermRef::DayTimeDurationLiteral(inner) => InternalTerm::DayTimeDurationLiteral(inner),
            InternalTermRef::TypedLiteral(inner) => InternalTerm::TypedLiteral(inner.into_owned()),
        }
    }
}

impl<'data> RdfValueRef<'data> for InternalTermRef<'data> {
    fn from_term(term: InternalTermRef<'data>) -> ThinResult<Self>
    where
        Self: Sized,
    {
        Ok(term)
    }
}

impl PartialOrd for InternalTermRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match *self {
            InternalTermRef::BlankNode(a) => Some(match other {
                InternalTermRef::BlankNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            InternalTermRef::NamedNode(a) => Some(match other {
                InternalTermRef::BlankNode(_) => Ordering::Greater,
                InternalTermRef::NamedNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            a => match other {
                InternalTermRef::NamedNode(_) | InternalTermRef::BlankNode(_) => Some(Ordering::Greater),
                _ => partial_cmp_literals(a, *other),
            },
        }
    }
}

fn partial_cmp_literals(a: InternalTermRef<'_>, b: InternalTermRef<'_>) -> Option<Ordering> {
    match a {
        InternalTermRef::SimpleLiteral(a) => {
            if let InternalTermRef::SimpleLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        InternalTermRef::LanguageStringLiteral(a) => {
            if let InternalTermRef::LanguageStringLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        InternalTermRef::NumericLiteral(a) => {
            if let InternalTermRef::NumericLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        InternalTermRef::DateTimeLiteral(a) => {
            if let InternalTermRef::DateTimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        InternalTermRef::TimeLiteral(a) => {
            if let InternalTermRef::TimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        InternalTermRef::DateLiteral(a) => {
            if let InternalTermRef::DateLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        InternalTermRef::DurationLiteral(a) => match b {
            InternalTermRef::DurationLiteral(b) => a.partial_cmp(&b),
            InternalTermRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            InternalTermRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        InternalTermRef::YearMonthDurationLiteral(a) => match b {
            InternalTermRef::DurationLiteral(b) => a.partial_cmp(&b),
            InternalTermRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            InternalTermRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        InternalTermRef::DayTimeDurationLiteral(a) => match b {
            InternalTermRef::DurationLiteral(b) => a.partial_cmp(&b),
            InternalTermRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            InternalTermRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        _ => None,
    }
}
