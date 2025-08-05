use rdf_fusion_model::{
    BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Duration, LanguageStringRef,
    LiteralRef, NamedNodeRef, Numeric, SimpleLiteralRef, TermRef, Time, TypedValueRef,
    YearMonthDuration,
};
use std::sync::Arc;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum EncodedTerm {
    NamedNode(Arc<str>),
    BlankNode(Arc<str>),
    TypedLiteral(Arc<str>, Arc<str>),
    LangString(Arc<str>, Arc<str>),
}

impl EncodedTerm {
    fn first_str(&self) -> &Arc<str> {
        match self {
            EncodedTerm::NamedNode(nn) => nn,
            EncodedTerm::BlankNode(bnode) => bnode,
            EncodedTerm::TypedLiteral(value, _) => value,
            EncodedTerm::LangString(value, _) => value,
        }
    }

    fn second_str(&self) -> Option<&Arc<str>> {
        match self {
            EncodedTerm::NamedNode(_) => None,
            EncodedTerm::BlankNode(_) => None,
            EncodedTerm::TypedLiteral(_, data_type) => Some(data_type),
            EncodedTerm::LangString(_, lang) => Some(lang),
        }
    }
}

impl<'term> From<&'term EncodedTerm> for TermRef<'term> {
    fn from(value: &'term EncodedTerm) -> Self {
        match value {
            EncodedTerm::NamedNode(nn) => {
                TermRef::NamedNode(NamedNodeRef::new_unchecked(nn.as_ref()))
            }
            EncodedTerm::BlankNode(bnode) => {
                TermRef::BlankNode(BlankNodeRef::new_unchecked(bnode.as_ref()))
            }
            EncodedTerm::TypedLiteral(value, data_type) => {
                TermRef::Literal(LiteralRef::new_typed_literal(
                    value.as_ref(),
                    NamedNodeRef::new_unchecked(data_type.as_ref()),
                ))
            }
            EncodedTerm::LangString(value, lang) => {
                TermRef::Literal(LiteralRef::new_language_tagged_literal_unchecked(
                    value.as_ref(),
                    lang.as_ref(),
                ))
            }
        }
    }
}

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum EncodedTypedValue {
    Invalid,
    NamedNode(Arc<str>),
    BlankNode(Arc<str>),
    BooleanLiteral(Boolean),
    NumericLiteral(Numeric),
    SimpleLiteral(Arc<str>),
    LanguageStringLiteral(Arc<str>, Arc<str>),
    DateTimeLiteral(DateTime),
    TimeLiteral(Time),
    DateLiteral(Date),
    DurationLiteral(Duration),
    YearMonthDurationLiteral(YearMonthDuration),
    DayTimeDurationLiteral(DayTimeDuration),
    OtherLiteral(Arc<str>, Arc<str>),
}

impl From<&EncodedTerm> for EncodedTypedValue {
    fn from(term: &EncodedTerm) -> Self {
        let term_ref = TermRef::from(term);
        let typed_value = TypedValueRef::try_from(term_ref);

        match typed_value {
            Ok(typed_value) => match typed_value {
                TypedValueRef::NamedNode(_) => {
                    EncodedTypedValue::NamedNode(term.first_str().clone())
                }
                TypedValueRef::BlankNode(_) => {
                    EncodedTypedValue::BlankNode(term.first_str().clone())
                }
                TypedValueRef::BooleanLiteral(lit) => {
                    EncodedTypedValue::BooleanLiteral(lit)
                }
                TypedValueRef::NumericLiteral(lit) => {
                    EncodedTypedValue::NumericLiteral(lit)
                }
                TypedValueRef::SimpleLiteral(_) => {
                    EncodedTypedValue::SimpleLiteral(term.first_str().clone())
                }
                TypedValueRef::LanguageStringLiteral(_) => {
                    EncodedTypedValue::LanguageStringLiteral(
                        term.first_str().clone(),
                        term.second_str()
                            .expect("Language strings have two strings")
                            .clone(),
                    )
                }
                TypedValueRef::DateTimeLiteral(lit) => {
                    EncodedTypedValue::DateTimeLiteral(lit)
                }
                TypedValueRef::TimeLiteral(lit) => EncodedTypedValue::TimeLiteral(lit),
                TypedValueRef::DateLiteral(lit) => EncodedTypedValue::DateLiteral(lit),
                TypedValueRef::DurationLiteral(lit) => {
                    EncodedTypedValue::DurationLiteral(lit)
                }
                TypedValueRef::YearMonthDurationLiteral(lit) => {
                    EncodedTypedValue::YearMonthDurationLiteral(lit)
                }
                TypedValueRef::DayTimeDurationLiteral(lit) => {
                    EncodedTypedValue::DayTimeDurationLiteral(lit)
                }
                TypedValueRef::OtherLiteral(_) => EncodedTypedValue::OtherLiteral(
                    term.first_str().clone(),
                    term.second_str()
                        .expect("Other typed literals have two strings")
                        .clone(),
                ),
            },
            Err(_) => Self::Invalid,
        }
    }
}

impl<'term> From<&'term EncodedTypedValue> for Option<TypedValueRef<'term>> {
    fn from(value: &'term EncodedTypedValue) -> Self {
        Some(match value {
            EncodedTypedValue::Invalid => return None,
            EncodedTypedValue::NamedNode(value) => {
                TypedValueRef::NamedNode(NamedNodeRef::new_unchecked(value.as_ref()))
            }
            EncodedTypedValue::BlankNode(value) => {
                TypedValueRef::BlankNode(BlankNodeRef::new_unchecked(value.as_ref()))
            }
            EncodedTypedValue::BooleanLiteral(lit) => TypedValueRef::BooleanLiteral(*lit),
            EncodedTypedValue::NumericLiteral(lit) => TypedValueRef::NumericLiteral(*lit),
            EncodedTypedValue::SimpleLiteral(lit) => {
                TypedValueRef::SimpleLiteral(SimpleLiteralRef::new(lit.as_ref()))
            }
            EncodedTypedValue::LanguageStringLiteral(value, lang) => {
                TypedValueRef::LanguageStringLiteral(LanguageStringRef::new(
                    value.as_ref(),
                    lang.as_ref(),
                ))
            }
            EncodedTypedValue::DateTimeLiteral(lit) => {
                TypedValueRef::DateTimeLiteral(*lit)
            }
            EncodedTypedValue::TimeLiteral(lit) => TypedValueRef::TimeLiteral(*lit),
            EncodedTypedValue::DateLiteral(lit) => TypedValueRef::DateLiteral(*lit),
            EncodedTypedValue::DurationLiteral(lit) => {
                TypedValueRef::DurationLiteral(*lit)
            }
            EncodedTypedValue::YearMonthDurationLiteral(lit) => {
                TypedValueRef::YearMonthDurationLiteral(*lit)
            }
            EncodedTypedValue::DayTimeDurationLiteral(lit) => {
                TypedValueRef::DayTimeDurationLiteral(*lit)
            }
            EncodedTypedValue::OtherLiteral(value, data_type) => {
                TypedValueRef::OtherLiteral(LiteralRef::new_typed_literal(
                    value.as_ref(),
                    NamedNodeRef::new_unchecked(data_type.as_ref()),
                ))
            }
        })
    }
}
