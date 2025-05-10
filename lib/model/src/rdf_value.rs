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
    pub fn as_ref(&self) -> TypedValueRef<'_> {
        match self {
            RdfTermValue::NamedNode(inner) => TypedValueRef::NamedNode(inner.as_ref()),
            RdfTermValue::BlankNode(inner) => TypedValueRef::BlankNode(inner.as_ref()),
            RdfTermValue::BooleanLiteral(inner) => TypedValueRef::BooleanLiteral(*inner),
            RdfTermValue::NumericLiteral(inner) => TypedValueRef::NumericLiteral(*inner),
            RdfTermValue::SimpleLiteral(inner) => TypedValueRef::SimpleLiteral(inner.as_ref()),
            RdfTermValue::LanguageStringLiteral(inner) => {
                TypedValueRef::LanguageStringLiteral(inner.as_ref())
            }
            RdfTermValue::DateTimeLiteral(inner) => TypedValueRef::DateTimeLiteral(*inner),
            RdfTermValue::TimeLiteral(inner) => TypedValueRef::TimeLiteral(*inner),
            RdfTermValue::DateLiteral(inner) => TypedValueRef::DateLiteral(*inner),
            RdfTermValue::DurationLiteral(inner) => TypedValueRef::DurationLiteral(*inner),
            RdfTermValue::YearMonthDurationLiteral(inner) => {
                TypedValueRef::YearMonthDurationLiteral(*inner)
            }
            RdfTermValue::DayTimeDurationLiteral(inner) => {
                TypedValueRef::DayTimeDurationLiteral(*inner)
            }
            RdfTermValue::OtherLiteral(inner) => TypedValueRef::OtherLiteral(inner.as_ref()),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum TypedValueRef<'value> {
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

impl TypedValueRef<'_> {
    /// Returns an owned decoded term.
    pub fn into_decoded(self) -> Term {
        match self {
            TypedValueRef::NamedNode(value) => Term::NamedNode(value.into_owned()),
            TypedValueRef::BlankNode(value) => Term::BlankNode(value.into_owned()),
            TypedValueRef::BooleanLiteral(value) => Term::Literal(Literal::from(value.as_bool())),
            TypedValueRef::NumericLiteral(value) => match value {
                Numeric::Int(value) => Term::Literal(Literal::from(i32::from(value))),
                Numeric::Integer(value) => Term::Literal(Literal::from(i64::from(value))),
                Numeric::Float(value) => Term::Literal(Literal::from(f32::from(value))),
                Numeric::Double(value) => Term::Literal(Literal::from(f64::from(value))),
                Numeric::Decimal(value) => {
                    Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::DECIMAL))
                }
            },
            TypedValueRef::SimpleLiteral(value) => Term::Literal(Literal::from(value.value)),
            TypedValueRef::LanguageStringLiteral(value) => Term::Literal(
                Literal::new_language_tagged_literal_unchecked(value.value, value.language),
            ),
            TypedValueRef::DateTimeLiteral(value) => Term::Literal(Literal::new_typed_literal(
                value.to_string(),
                xsd::DATE_TIME,
            )),
            TypedValueRef::TimeLiteral(value) => {
                Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::TIME))
            }
            TypedValueRef::DateLiteral(value) => {
                Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::DATE))
            }
            TypedValueRef::DurationLiteral(value) => {
                Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::DURATION))
            }
            TypedValueRef::YearMonthDurationLiteral(value) => Term::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::YEAR_MONTH_DURATION),
            ),
            TypedValueRef::DayTimeDurationLiteral(value) => Term::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::DAY_TIME_DURATION),
            ),
            TypedValueRef::OtherLiteral(value) => Term::Literal(value.into_owned()),
        }
    }

    pub fn into_owned(self) -> RdfTermValue {
        match self {
            TypedValueRef::NamedNode(inner) => RdfTermValue::NamedNode(inner.into_owned()),
            TypedValueRef::BlankNode(inner) => RdfTermValue::BlankNode(inner.into_owned()),
            TypedValueRef::BooleanLiteral(inner) => RdfTermValue::BooleanLiteral(inner),
            TypedValueRef::NumericLiteral(inner) => RdfTermValue::NumericLiteral(inner),
            TypedValueRef::SimpleLiteral(inner) => RdfTermValue::SimpleLiteral(inner.into_owned()),
            TypedValueRef::LanguageStringLiteral(inner) => {
                RdfTermValue::LanguageStringLiteral(inner.into_owned())
            }
            TypedValueRef::DateTimeLiteral(inner) => RdfTermValue::DateTimeLiteral(inner),
            TypedValueRef::TimeLiteral(inner) => RdfTermValue::TimeLiteral(inner),
            TypedValueRef::DateLiteral(inner) => RdfTermValue::DateLiteral(inner),
            TypedValueRef::DurationLiteral(inner) => RdfTermValue::DurationLiteral(inner),
            TypedValueRef::YearMonthDurationLiteral(inner) => {
                RdfTermValue::YearMonthDurationLiteral(inner)
            }
            TypedValueRef::DayTimeDurationLiteral(inner) => {
                RdfTermValue::DayTimeDurationLiteral(inner)
            }
            TypedValueRef::OtherLiteral(inner) => RdfTermValue::OtherLiteral(inner.into_owned()),
        }
    }
}

impl PartialOrd for TypedValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match *self {
            TypedValueRef::BlankNode(a) => Some(match other {
                TypedValueRef::BlankNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            TypedValueRef::NamedNode(a) => Some(match other {
                TypedValueRef::BlankNode(_) => Ordering::Greater,
                TypedValueRef::NamedNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            a => match other {
                TypedValueRef::NamedNode(_) | TypedValueRef::BlankNode(_) => Some(Ordering::Greater),
                _ => partial_cmp_literals(a, *other),
            },
        }
    }
}

fn partial_cmp_literals(a: TypedValueRef<'_>, b: TypedValueRef<'_>) -> Option<Ordering> {
    match a {
        TypedValueRef::SimpleLiteral(a) => {
            if let TypedValueRef::SimpleLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::LanguageStringLiteral(a) => {
            if let TypedValueRef::LanguageStringLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::NumericLiteral(a) => {
            if let TypedValueRef::NumericLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::DateTimeLiteral(a) => {
            if let TypedValueRef::DateTimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::TimeLiteral(a) => {
            if let TypedValueRef::TimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::DateLiteral(a) => {
            if let TypedValueRef::DateLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::DurationLiteral(a) => match b {
            TypedValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TypedValueRef::YearMonthDurationLiteral(a) => match b {
            TypedValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TypedValueRef::DayTimeDurationLiteral(a) => match b {
            TypedValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        _ => None,
    }
}

macro_rules! impl_from {
    ($TYPE: ty, $VARIANT: path) => {
        impl<'data> From<$TYPE> for TypedValueRef<'data> {
            fn from(value: $TYPE) -> Self {
                $VARIANT(value)
            }
        }
    };
}

impl_from!(Boolean, TypedValueRef::BooleanLiteral);
impl_from!(Numeric, TypedValueRef::NumericLiteral);
impl_from!(SimpleLiteralRef<'data>, TypedValueRef::SimpleLiteral);
impl_from!(LanguageStringRef<'data>, TypedValueRef::LanguageStringLiteral);
impl_from!(LiteralRef<'data>, TypedValueRef::OtherLiteral);
