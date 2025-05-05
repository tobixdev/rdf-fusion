use crate::{
    Boolean, Date, DateTime, DayTimeDuration, Duration, LanguageString, LanguageStringRef, Numeric,
    SimpleLiteral, SimpleLiteralRef, Term, Time, YearMonthDuration,
};
use oxrdf::vocab::xsd;
use oxrdf::{BlankNode, BlankNodeRef, Literal, LiteralRef, NamedNode, NamedNodeRef};
use std::cmp::Ordering;

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub enum RdfTermValue {
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
    OtherLiteral(Literal),
}

impl RdfTermValue {
    pub fn as_ref(&self) -> TermValueRef<'_> {
        match self {
            RdfTermValue::NamedNode(inner) => TermValueRef::NamedNode(inner.as_ref()),
            RdfTermValue::BlankNode(inner) => TermValueRef::BlankNode(inner.as_ref()),
            RdfTermValue::BooleanLiteral(inner) => TermValueRef::BooleanLiteral(*inner),
            RdfTermValue::NumericLiteral(inner) => TermValueRef::NumericLiteral(*inner),
            RdfTermValue::SimpleLiteral(inner) => TermValueRef::SimpleLiteral(inner.as_ref()),
            RdfTermValue::LanguageStringLiteral(inner) => {
                TermValueRef::LanguageStringLiteral(inner.as_ref())
            }
            RdfTermValue::DateTimeLiteral(inner) => TermValueRef::DateTimeLiteral(*inner),
            RdfTermValue::TimeLiteral(inner) => TermValueRef::TimeLiteral(*inner),
            RdfTermValue::DateLiteral(inner) => TermValueRef::DateLiteral(*inner),
            RdfTermValue::DurationLiteral(inner) => TermValueRef::DurationLiteral(*inner),
            RdfTermValue::YearMonthDurationLiteral(inner) => {
                TermValueRef::YearMonthDurationLiteral(*inner)
            }
            RdfTermValue::DayTimeDurationLiteral(inner) => {
                TermValueRef::DayTimeDurationLiteral(*inner)
            }
            RdfTermValue::OtherLiteral(inner) => TermValueRef::OtherLiteral(inner.as_ref()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum TermValueRef<'value> {
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
    OtherLiteral(LiteralRef<'value>),
}

impl TermValueRef<'_> {
    /// Returns an owned decoded term.
    pub fn into_decoded(self) -> Term {
        match self {
            TermValueRef::NamedNode(value) => Term::NamedNode(value.into_owned()),
            TermValueRef::BlankNode(value) => Term::BlankNode(value.into_owned()),
            TermValueRef::BooleanLiteral(value) => Term::Literal(Literal::from(value.as_bool())),
            TermValueRef::NumericLiteral(value) => match value {
                Numeric::Int(value) => Term::Literal(Literal::from(i32::from(value))),
                Numeric::Integer(value) => Term::Literal(Literal::from(i64::from(value))),
                Numeric::Float(value) => Term::Literal(Literal::from(f32::from(value))),
                Numeric::Double(value) => Term::Literal(Literal::from(f64::from(value))),
                Numeric::Decimal(value) => {
                    Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::DECIMAL))
                }
            },
            TermValueRef::SimpleLiteral(value) => Term::Literal(Literal::from(value.value)),
            TermValueRef::LanguageStringLiteral(value) => Term::Literal(
                Literal::new_language_tagged_literal_unchecked(value.value, value.language),
            ),
            TermValueRef::DateTimeLiteral(value) => Term::Literal(Literal::new_typed_literal(
                value.to_string(),
                xsd::DATE_TIME,
            )),
            TermValueRef::TimeLiteral(value) => {
                Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::TIME))
            }
            TermValueRef::DateLiteral(value) => {
                Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::DATE))
            }
            TermValueRef::DurationLiteral(value) => {
                Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::DURATION))
            }
            TermValueRef::YearMonthDurationLiteral(value) => Term::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::YEAR_MONTH_DURATION),
            ),
            TermValueRef::DayTimeDurationLiteral(value) => Term::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::DAY_TIME_DURATION),
            ),
            TermValueRef::OtherLiteral(value) => Term::Literal(value.into_owned()),
        }
    }

    pub fn into_owned(self) -> RdfTermValue {
        match self {
            TermValueRef::NamedNode(inner) => RdfTermValue::NamedNode(inner.into_owned()),
            TermValueRef::BlankNode(inner) => RdfTermValue::BlankNode(inner.into_owned()),
            TermValueRef::BooleanLiteral(inner) => RdfTermValue::BooleanLiteral(inner),
            TermValueRef::NumericLiteral(inner) => RdfTermValue::NumericLiteral(inner),
            TermValueRef::SimpleLiteral(inner) => RdfTermValue::SimpleLiteral(inner.into_owned()),
            TermValueRef::LanguageStringLiteral(inner) => {
                RdfTermValue::LanguageStringLiteral(inner.into_owned())
            }
            TermValueRef::DateTimeLiteral(inner) => RdfTermValue::DateTimeLiteral(inner),
            TermValueRef::TimeLiteral(inner) => RdfTermValue::TimeLiteral(inner),
            TermValueRef::DateLiteral(inner) => RdfTermValue::DateLiteral(inner),
            TermValueRef::DurationLiteral(inner) => RdfTermValue::DurationLiteral(inner),
            TermValueRef::YearMonthDurationLiteral(inner) => {
                RdfTermValue::YearMonthDurationLiteral(inner)
            }
            TermValueRef::DayTimeDurationLiteral(inner) => {
                RdfTermValue::DayTimeDurationLiteral(inner)
            }
            TermValueRef::OtherLiteral(inner) => RdfTermValue::OtherLiteral(inner.into_owned()),
        }
    }
}

impl PartialOrd for TermValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match *self {
            TermValueRef::BlankNode(a) => Some(match other {
                TermValueRef::BlankNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            TermValueRef::NamedNode(a) => Some(match other {
                TermValueRef::BlankNode(_) => Ordering::Greater,
                TermValueRef::NamedNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            a => match other {
                TermValueRef::NamedNode(_) | TermValueRef::BlankNode(_) => Some(Ordering::Greater),
                _ => partial_cmp_literals(a, *other),
            },
        }
    }
}

fn partial_cmp_literals(a: TermValueRef<'_>, b: TermValueRef<'_>) -> Option<Ordering> {
    match a {
        TermValueRef::SimpleLiteral(a) => {
            if let TermValueRef::SimpleLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermValueRef::LanguageStringLiteral(a) => {
            if let TermValueRef::LanguageStringLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermValueRef::NumericLiteral(a) => {
            if let TermValueRef::NumericLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermValueRef::DateTimeLiteral(a) => {
            if let TermValueRef::DateTimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermValueRef::TimeLiteral(a) => {
            if let TermValueRef::TimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermValueRef::DateLiteral(a) => {
            if let TermValueRef::DateLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TermValueRef::DurationLiteral(a) => match b {
            TermValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TermValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TermValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TermValueRef::YearMonthDurationLiteral(a) => match b {
            TermValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TermValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TermValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TermValueRef::DayTimeDurationLiteral(a) => match b {
            TermValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TermValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TermValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        _ => None,
    }
}

macro_rules! impl_from {
    ($TYPE: ty, $VARIANT: path) => {
        impl<'data> From<$TYPE> for TermValueRef<'data> {
            fn from(value: $TYPE) -> Self {
                $VARIANT(value)
            }
        }
    };
}

impl_from!(Boolean, TermValueRef::BooleanLiteral);
impl_from!(Numeric, TermValueRef::NumericLiteral);
impl_from!(SimpleLiteralRef<'data>, TermValueRef::SimpleLiteral);
impl_from!(LanguageStringRef<'data>, TermValueRef::LanguageStringLiteral);
impl_from!(LiteralRef<'data>, TermValueRef::OtherLiteral);
