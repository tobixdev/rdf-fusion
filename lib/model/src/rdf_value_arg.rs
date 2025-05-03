use crate::TermValueRef;
use crate::{
    Boolean, DateTime, Decimal, Double, Duration, Float, Int, Integer, LanguageStringRef, Numeric,
    SimpleLiteralRef, StringLiteralRef, ThinError, ThinResult,
};
use oxrdf::{BlankNodeRef, LiteralRef, NamedNodeRef};

pub trait RdfTermValueArg<'data>: Copy {
    fn try_from_value(value: TermValueRef<'data>) -> ThinResult<Self>;
}

impl<'data> RdfTermValueArg<'data> for TermValueRef<'data> {
    fn try_from_value(value: TermValueRef<'data>) -> ThinResult<Self> {
        Ok(value)
    }
}

impl<'data> RdfTermValueArg<'data> for NamedNodeRef<'data> {
    fn try_from_value(value: TermValueRef<'data>) -> ThinResult<Self> {
        match value {
            TermValueRef::NamedNode(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> RdfTermValueArg<'data> for BlankNodeRef<'data> {
    fn try_from_value(value: TermValueRef<'data>) -> ThinResult<Self> {
        match value {
            TermValueRef::BlankNode(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for Boolean {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::BooleanLiteral(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for Int {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::NumericLiteral(Numeric::Int(inner)) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for Integer {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::NumericLiteral(Numeric::Integer(inner)) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for Float {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::NumericLiteral(Numeric::Float(inner)) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for Double {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::NumericLiteral(Numeric::Double(inner)) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for Decimal {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::NumericLiteral(Numeric::Decimal(inner)) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for Numeric {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::NumericLiteral(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> RdfTermValueArg<'data> for SimpleLiteralRef<'data> {
    fn try_from_value(value: TermValueRef<'data>) -> ThinResult<Self> {
        match value {
            TermValueRef::SimpleLiteral(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> RdfTermValueArg<'data> for LanguageStringRef<'data> {
    fn try_from_value(value: TermValueRef<'data>) -> ThinResult<Self> {
        match value {
            TermValueRef::LanguageStringLiteral(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> RdfTermValueArg<'data> for StringLiteralRef<'data> {
    fn try_from_value(value: TermValueRef<'data>) -> ThinResult<Self> {
        match value {
            TermValueRef::SimpleLiteral(inner) => Ok(StringLiteralRef(inner.value, None)),
            TermValueRef::LanguageStringLiteral(inner) => {
                Ok(StringLiteralRef(inner.value, Some(inner.language)))
            }
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for Duration {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::DurationLiteral(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl RdfTermValueArg<'_> for DateTime {
    fn try_from_value(value: TermValueRef<'_>) -> ThinResult<Self> {
        match value {
            TermValueRef::DateTimeLiteral(inner) => Ok(inner),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> RdfTermValueArg<'data> for LiteralRef<'data> {
    fn try_from_value(value: TermValueRef<'data>) -> ThinResult<Self> {
        match value {
            TermValueRef::SimpleLiteral(inner) => {
                Ok(LiteralRef::new_simple_literal(inner.value))
            }
            TermValueRef::LanguageStringLiteral(inner) => Ok(
                LiteralRef::new_language_tagged_literal_unchecked(inner.value, inner.language),
            ),
            _ => ThinError::expected(),
        }
    }
}
