use crate::encoding::TermEncoder;
use crate::value_encoding::{TermValueEncoding, ValueArrayBuilder};
use crate::DFResult;
use datafusion::common::exec_err;
use model::{
    BlankNode, BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration,
    Float, Int, Integer, LanguageStringRef, LiteralRef, NamedNode, NamedNodeRef, Numeric,
    OwnedStringLiteral, SimpleLiteralRef, StringLiteralRef, TermValueRef, ThinError, ThinResult,
    Time, YearMonthDuration,
};

impl<'data> TermEncoder<TermValueRef<'data>> for TermValueEncoding {
    fn encode_terms(
        terms: impl IntoIterator<Item = ThinResult<TermValueRef<'data>>>,
    ) -> DFResult<Self::Array> {
        let mut value_builder = ValueArrayBuilder::default();
        for value in terms {
            match value {
                Ok(TermValueRef::NamedNode(value)) => value_builder.append_named_node(value)?,
                Ok(TermValueRef::BlankNode(value)) => value_builder.append_blank_node(value)?,
                Ok(TermValueRef::BooleanLiteral(value)) => {
                    value_builder.append_boolean(value.as_bool())?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Float(value))) => {
                    value_builder.append_float(value)?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Double(value))) => {
                    value_builder.append_double(value)?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Decimal(value))) => {
                    value_builder.append_decimal(value)?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Int(value))) => {
                    value_builder.append_int(value)?
                }
                Ok(TermValueRef::NumericLiteral(Numeric::Integer(value))) => {
                    value_builder.append_integer(value)?
                }
                Ok(TermValueRef::SimpleLiteral(value)) => {
                    value_builder.append_string(value.value, None)?
                }
                Ok(TermValueRef::LanguageStringLiteral(value)) => {
                    value_builder.append_string(value.value, Some(value.language))?
                }
                Ok(TermValueRef::DateTimeLiteral(value)) => {
                    value_builder.append_date_time(value)?
                }
                Ok(TermValueRef::TimeLiteral(value)) => value_builder.append_time(value)?,
                Ok(TermValueRef::DateLiteral(value)) => value_builder.append_date(value)?,
                Ok(TermValueRef::DurationLiteral(value)) => value_builder
                    .append_duration(Some(value.year_month()), Some(value.day_time()))?,
                Ok(TermValueRef::YearMonthDurationLiteral(value)) => {
                    value_builder.append_duration(Some(value), None)?
                }
                Ok(TermValueRef::DayTimeDurationLiteral(value)) => {
                    value_builder.append_duration(None, Some(value))?
                }
                Ok(TermValueRef::OtherLiteral(value)) => {
                    value_builder.append_typed_literal(value)?
                }
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Self::Array::try_from(value_builder.finish())
    }

    fn encode_term(term: ThinResult<TermValueRef<'data>>) -> DFResult<Self::Scalar> {
        todo!()
    }
}

macro_rules! make_enum_based_encoder {
    ($TYPE: ty, $VARIANT: path) => {
        impl<'data> TermEncoder<$TYPE> for TermValueEncoding {
            fn encode_terms(
                terms: impl IntoIterator<Item = ThinResult<$TYPE>>,
            ) -> DFResult<Self::Array> {
                let terms = terms.into_iter().map(|t| t.map($VARIANT));
                Self::encode_terms(terms)
            }

            fn encode_term(term: ThinResult<$TYPE>) -> DFResult<Self::Scalar> {
                let term = term.map($VARIANT);
                Self::encode_term(term)
            }
        }
    };
}

make_enum_based_encoder!(NamedNodeRef<'data>, TermValueRef::NamedNode);
make_enum_based_encoder!(BlankNodeRef<'data>, TermValueRef::BlankNode);
make_enum_based_encoder!(Boolean, TermValueRef::BooleanLiteral);
make_enum_based_encoder!(Numeric, TermValueRef::NumericLiteral);
make_enum_based_encoder!(SimpleLiteralRef<'data>, TermValueRef::SimpleLiteral);
make_enum_based_encoder!(
    LanguageStringRef<'data>,
    TermValueRef::LanguageStringLiteral
);
make_enum_based_encoder!(DateTime, TermValueRef::DateTimeLiteral);
make_enum_based_encoder!(Time, TermValueRef::TimeLiteral);
make_enum_based_encoder!(Date, TermValueRef::DateLiteral);
make_enum_based_encoder!(Duration, TermValueRef::DurationLiteral);
make_enum_based_encoder!(YearMonthDuration, TermValueRef::YearMonthDurationLiteral);
make_enum_based_encoder!(DayTimeDuration, TermValueRef::DayTimeDurationLiteral);

make_enum_based_encoder!(Float, Numeric::Float);
make_enum_based_encoder!(Double, Numeric::Double);
make_enum_based_encoder!(Integer, Numeric::Integer);
make_enum_based_encoder!(Int, Numeric::Int);
make_enum_based_encoder!(Decimal, Numeric::Decimal);

impl<'data> TermEncoder<NamedNode> for TermValueEncoding {
    fn encode_terms(terms: impl IntoIterator<Item = ThinResult<NamedNode>>) -> DFResult<Self::Array> {
        let mut value_builder = ValueArrayBuilder::default();
        for value in terms {
            match value {
                Ok(value) => value_builder.append_named_node(value.as_ref())?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Self::Array::try_from(value_builder.finish())
    }

    fn encode_term(term: ThinResult<NamedNode>) -> DFResult<Self::Scalar> {
        todo!()
    }
}

impl<'data> TermEncoder<BlankNode> for TermValueEncoding {
    fn encode_terms(terms: impl IntoIterator<Item = ThinResult<BlankNode>>) -> DFResult<Self::Array> {
        let mut value_builder = ValueArrayBuilder::default();
        for value in terms {
            match value {
                Ok(value) => value_builder.append_blank_node(value.as_ref())?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Self::Array::try_from(value_builder.finish())
    }

    fn encode_term(term: ThinResult<BlankNode>) -> DFResult<Self::Scalar> {
        todo!()
    }
}

impl<'data> TermEncoder<StringLiteralRef<'data>> for TermValueEncoding {
    fn encode_terms(
        terms: impl IntoIterator<Item = ThinResult<StringLiteralRef<'data>>>,
    ) -> DFResult<Self::Array> {
        let terms = terms.into_iter().map(|result| {
            result.map(|t| match t.1 {
                None => TermValueRef::SimpleLiteral(SimpleLiteralRef::new(t.0)),
                Some(language) => {
                    TermValueRef::LanguageStringLiteral(LanguageStringRef::new(t.0, language))
                }
            })
        });
        Self::encode_terms(terms)
    }

    fn encode_term(term: ThinResult<StringLiteralRef<'data>>) -> DFResult<Self::Scalar> {
        todo!()
    }
}

impl<'data> TermEncoder<OwnedStringLiteral> for TermValueEncoding {
    fn encode_terms(
        terms: impl IntoIterator<Item = ThinResult<OwnedStringLiteral>>,
    ) -> DFResult<Self::Array> {
        let mut value_builder = ValueArrayBuilder::default();
        for value in terms {
            match value {
                Ok(value) => value_builder.append_string(&value.0, value.1.as_deref())?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Self::Array::try_from(value_builder.finish())
    }

    fn encode_term(term: ThinResult<OwnedStringLiteral>) -> DFResult<Self::Scalar> {
        todo!()
    }
}

impl<'data> TermEncoder<LiteralRef<'data>> for TermValueEncoding {
    fn encode_terms(
        terms: impl IntoIterator<Item = ThinResult<LiteralRef<'data>>>,
    ) -> DFResult<Self::Array> {
        let mut value_builder = ValueArrayBuilder::default();
        for value in terms {
            match value {
                Ok(value) => value_builder.append_typed_literal(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Self::Array::try_from(value_builder.finish())
    }

    fn encode_term(term: ThinResult<LiteralRef<'data>>) -> DFResult<Self::Scalar> {
        todo!()
    }
}
