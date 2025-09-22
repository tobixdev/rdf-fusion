use crate::memory::encoding::EncodedTerm;
use rdf_fusion_model::{
    BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Duration, LanguageStringRef,
    LiteralRef, NamedNodeRef, Numeric, SimpleLiteralRef, TermRef, Time, TypedValueRef,
    YearMonthDuration,
};
use std::sync::Arc;

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
                    EncodedTypedValue::NamedNode(Arc::clone(term.first_str()))
                }
                TypedValueRef::BlankNode(_) => {
                    EncodedTypedValue::BlankNode(Arc::clone(term.first_str()))
                }
                TypedValueRef::BooleanLiteral(lit) => {
                    EncodedTypedValue::BooleanLiteral(lit)
                }
                TypedValueRef::NumericLiteral(lit) => {
                    EncodedTypedValue::NumericLiteral(lit)
                }
                TypedValueRef::SimpleLiteral(_) => {
                    EncodedTypedValue::SimpleLiteral(Arc::clone(term.first_str()))
                }
                TypedValueRef::LanguageStringLiteral(_) => {
                    EncodedTypedValue::LanguageStringLiteral(
                        Arc::clone(term.first_str()),
                        Arc::clone(
                            term.second_str()
                                .expect("Language strings have two strings"),
                        ),
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
                    Arc::clone(term.first_str()),
                    Arc::clone(
                        term.second_str()
                            .expect("Other typed literals have two strings"),
                    ),
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
