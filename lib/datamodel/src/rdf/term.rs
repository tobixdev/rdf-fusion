use crate::rdf::language_string::LanguageString;
use crate::rdf::simple_literal::SimpleLiteral;
use crate::rdf::typed_literal::TypedLiteral;
use crate::{
    Boolean, Date, DateTime, DayTimeDuration, DecodedTerm, Duration, LanguageStringRef, Numeric,
    RdfOpResult, RdfValueRef, SimpleLiteralRef, Time, TypedLiteralRef, YearMonthDuration,
};
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, BlankNodeRef, Literal, NamedNode, NamedNodeRef};
use std::cmp::Ordering;

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub enum Term {
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

impl Term {
    pub fn as_ref(&self) -> TermRef<'_> {
        match self {
            Term::NamedNode(inner) => TermRef::NamedNode(inner.as_ref()),
            Term::BlankNode(inner) => TermRef::BlankNode(inner.as_ref()),
            Term::BooleanLiteral(inner) => TermRef::BooleanLiteral(*inner),
            Term::NumericLiteral(inner) => TermRef::NumericLiteral(*inner),
            Term::SimpleLiteral(inner) => TermRef::SimpleLiteral(inner.as_ref()),
            Term::LanguageStringLiteral(inner) => TermRef::LanguageStringLiteral(inner.as_ref()),
            Term::DateTimeLiteral(inner) => TermRef::DateTimeLiteral(*inner),
            Term::TimeLiteral(inner) => TermRef::TimeLiteral(*inner),
            Term::DateLiteral(inner) => TermRef::DateLiteral(*inner),
            Term::DurationLiteral(inner) => TermRef::DurationLiteral(*inner),
            Term::YearMonthDurationLiteral(inner) => TermRef::YearMonthDurationLiteral(*inner),
            Term::DayTimeDurationLiteral(inner) => TermRef::DayTimeDurationLiteral(*inner),
            Term::TypedLiteral(inner) => TermRef::TypedLiteral(inner.as_ref()),
        }
    }
}

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

    pub fn to_owned(&self) -> Term {
        match self {
            TermRef::NamedNode(inner) => Term::NamedNode(inner.into_owned()),
            TermRef::BlankNode(inner) => Term::BlankNode(inner.into_owned()),
            TermRef::BooleanLiteral(inner) => Term::BooleanLiteral(*inner),
            TermRef::NumericLiteral(inner) => Term::NumericLiteral(*inner),
            TermRef::SimpleLiteral(inner) => Term::SimpleLiteral(inner.to_owned()),
            TermRef::LanguageStringLiteral(inner) => Term::LanguageStringLiteral(inner.to_owned()),
            TermRef::DateTimeLiteral(inner) => Term::DateTimeLiteral(*inner),
            TermRef::TimeLiteral(inner) => Term::TimeLiteral(*inner),
            TermRef::DateLiteral(inner) => Term::DateLiteral(*inner),
            TermRef::DurationLiteral(inner) => Term::DurationLiteral(*inner),
            TermRef::YearMonthDurationLiteral(inner) => Term::YearMonthDurationLiteral(*inner),
            TermRef::DayTimeDurationLiteral(inner) => Term::DayTimeDurationLiteral(*inner),
            TermRef::TypedLiteral(inner) => Term::TypedLiteral(inner.to_owned()),
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

impl PartialOrd for TermRef<'_> {
    fn partial_cmp(&self, b: &Self) -> Option<Ordering> {
        match *self {
            TermRef::BlankNode(a) => Some(match b {
                TermRef::BlankNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            TermRef::NamedNode(a) => Some(match b {
                TermRef::BlankNode(_) => Ordering::Greater,
                TermRef::NamedNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            a => match b {
                TermRef::NamedNode(_) | TermRef::BlankNode(_) => Some(Ordering::Greater),
                _ => partial_cmp_literals(a, *b),
            },
        }
    }
}

fn partial_cmp_literals(a: TermRef<'_>, b: TermRef<'_>) -> Option<Ordering> {
    match a {
        TermRef::SimpleLiteral(a) => {
            if let TermRef::SimpleLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermRef::LanguageStringLiteral(a) => {
            if let TermRef::LanguageStringLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermRef::NumericLiteral(a) => {
            if let TermRef::NumericLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermRef::DateTimeLiteral(a) => {
            if let TermRef::DateTimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermRef::TimeLiteral(a) => {
            if let TermRef::TimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermRef::DateLiteral(a) => {
            if let TermRef::DateLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermRef::DurationLiteral(a) => match b {
            TermRef::DurationLiteral(b) => a.partial_cmp(&b),
            TermRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TermRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TermRef::YearMonthDurationLiteral(a) => match b {
            TermRef::DurationLiteral(b) => a.partial_cmp(&b),
            TermRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TermRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TermRef::DayTimeDurationLiteral(a) => match b {
            TermRef::DurationLiteral(b) => a.partial_cmp(&b),
            TermRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TermRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        _ => None,
    }
}
