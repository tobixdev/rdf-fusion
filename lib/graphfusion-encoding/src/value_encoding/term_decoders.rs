use crate::encoding::{EncodingArray, TermDecoder};
use crate::value_encoding::array::{TermValueArrayParts, TimestampParts};
use crate::value_encoding::{TermValueEncoding, ValueEncodingField};
use datafusion::arrow::array::Array;
use model::{
    BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int,
    Integer, LanguageStringRef, LiteralRef, NamedNodeRef, Numeric, SimpleLiteralRef,
    StringLiteralRef, TermValueRef, ThinError, ThinResult, Time, Timestamp, TimezoneOffset,
    YearMonthDuration,
};
use std::ops::Not;

/// Extracts a sequence of term references from the given array.
impl<'data> TermDecoder<'data, TermValueRef<'data>> for TermValueEncoding {
    fn decode_terms(
        array: &'data Self::Array,
    ) -> impl Iterator<Item = ThinResult<TermValueRef<'data>>> {
        let parts = array.parts_as_ref();
        (0..array.array().len()).map(move |idx| extract_term_value(&parts, idx))
    }

    fn decode_term(scalar: &'data Self::Scalar) -> ThinResult<TermValueRef<'data>> {
        todo!()
    }
}

macro_rules! extract_from_term_value {
    ($TYPE: ty, $VARIANT: path) => {
        impl<'data> TermDecoder<'data, $TYPE> for TermValueEncoding {
            fn decode_terms(array: &'data Self::Array) -> impl Iterator<Item = ThinResult<$TYPE>> {
                <Self as TermDecoder<TermValueRef<'data>>>::decode_terms(array).map(|r| match r {
                    Ok($VARIANT(numeric)) => Ok(numeric),
                    Ok(_) => ThinError::expected(),
                    Err(err) => Err(err),
                })
            }

            fn decode_term(array: &'data Self::Scalar) -> ThinResult<$TYPE> {
                todo!()
            }
        }
    };
}

extract_from_term_value!(NamedNodeRef<'data>, TermValueRef::NamedNode);
extract_from_term_value!(Numeric, TermValueRef::NumericLiteral);
extract_from_term_value!(SimpleLiteralRef<'data>, TermValueRef::SimpleLiteral);
extract_from_term_value!(DateTime, TermValueRef::DateTimeLiteral);
extract_from_term_value!(Date, TermValueRef::DateLiteral);
extract_from_term_value!(Time, TermValueRef::TimeLiteral);

impl<'data> TermDecoder<'data, StringLiteralRef<'data>> for TermValueEncoding {
    fn decode_terms(
        array: &'data Self::Array,
    ) -> impl Iterator<Item = ThinResult<StringLiteralRef<'data>>> {
        <Self as TermDecoder<TermValueRef<'data>>>::decode_terms(array).map(|r| match r {
            Ok(TermValueRef::SimpleLiteral(value)) => Ok(StringLiteralRef(value.value, None)),
            Ok(TermValueRef::LanguageStringLiteral(value)) => {
                Ok(StringLiteralRef(value.value, Some(value.language)))
            }
            Ok(_) => ThinError::expected(),
            Err(err) => Err(err),
        })
    }

    fn decode_term(array: &'data Self::Scalar) -> ThinResult<StringLiteralRef<'data>> {
        todo!()
    }
}
impl<'data> TermDecoder<'data, Integer> for TermValueEncoding {
    fn decode_terms(array: &'data Self::Array) -> impl Iterator<Item = ThinResult<Integer>> {
        <Self as TermDecoder<Numeric>>::decode_terms(array).map(|r| match r {
            Ok(Numeric::Int(value)) => Ok(value.into()),
            Ok(Numeric::Integer(value)) => Ok(value),
            Ok(_) => ThinError::expected(),
            Err(err) => Err(err),
        })
    }

    fn decode_term(array: &'data Self::Scalar) -> ThinResult<Integer> {
        todo!()
    }
}

fn extract_term_value<'data>(
    parts: &TermValueArrayParts<'data>,
    idx: usize,
) -> ThinResult<TermValueRef<'data>> {
    let field = ValueEncodingField::try_from(parts.array.type_id(idx))
        .map_err(|_| ThinError::InternalError("Unexpected type id"))?;
    let offset = parts.array.value_offset(idx);

    match field {
        ValueEncodingField::Null => ThinError::expected(),
        ValueEncodingField::NamedNode => {
            let value = NamedNodeRef::new_unchecked(parts.named_nodes.value(offset));
            Ok(TermValueRef::NamedNode(value))
        }
        ValueEncodingField::BlankNode => {
            let value = BlankNodeRef::new_unchecked(parts.blank_nodes.value(offset));
            Ok(TermValueRef::BlankNode(value))
        }
        ValueEncodingField::String => {
            if parts.strings.language.is_null(offset) {
                Ok(TermValueRef::SimpleLiteral(SimpleLiteralRef::new(
                    parts.strings.value.value(offset),
                )))
            } else {
                Ok(TermValueRef::LanguageStringLiteral(LanguageStringRef::new(
                    parts.strings.value.value(offset),
                    parts.strings.language.value(offset),
                )))
            }
        }
        ValueEncodingField::Boolean => {
            let value = Boolean::from(parts.booleans.value(offset));
            Ok(TermValueRef::BooleanLiteral(value))
        }
        ValueEncodingField::Float => {
            let value = Float::from(parts.floats.value(offset));
            Ok(TermValueRef::NumericLiteral(Numeric::Float(value)))
        }
        ValueEncodingField::Double => {
            let value = Double::from(parts.doubles.value(offset));
            Ok(TermValueRef::NumericLiteral(Numeric::Double(value)))
        }
        ValueEncodingField::Decimal => {
            let value = Decimal::from_be_bytes(parts.decimals.value(offset).to_be_bytes());
            Ok(TermValueRef::NumericLiteral(Numeric::Decimal(value)))
        }
        ValueEncodingField::Int => {
            let value = Int::from(parts.ints.value(offset));
            Ok(TermValueRef::NumericLiteral(Numeric::Int(value)))
        }
        ValueEncodingField::Integer => {
            let value = Integer::from(parts.integers.value(offset));
            Ok(TermValueRef::NumericLiteral(Numeric::Integer(value)))
        }
        ValueEncodingField::DateTime => {
            let timestamp = extract_timestamp(parts.date_times, offset);
            Ok(TermValueRef::DateTimeLiteral(DateTime::new(timestamp)))
        }
        ValueEncodingField::Time => {
            let timestamp = extract_timestamp(parts.times, offset);
            Ok(TermValueRef::TimeLiteral(Time::new(timestamp)))
        }
        ValueEncodingField::Date => {
            let timestamp = extract_timestamp(parts.dates, offset);
            Ok(TermValueRef::DateLiteral(Date::new(timestamp)))
        }
        ValueEncodingField::Duration => extract_duration(parts, offset),
        ValueEncodingField::OtherLiteral => {
            let value = parts.other_literals.value.value(offset);
            let datatype = parts.other_literals.datatype.value(offset);
            let datatype = NamedNodeRef::new_unchecked(datatype);
            Ok(TermValueRef::OtherLiteral(LiteralRef::new_typed_literal(
                value, datatype,
            )))
        }
    }
}

fn extract_timestamp(parts: TimestampParts<'_>, offset: usize) -> Timestamp {
    Timestamp::new(
        Decimal::from_be_bytes(parts.value.value(offset).to_be_bytes()),
        parts
            .offset
            .is_null(offset)
            .not()
            .then(|| TimezoneOffset::new_unchecked(parts.offset.value(offset))),
    )
}

fn extract_duration<'data>(
    parts: &TermValueArrayParts<'_>,
    offset: usize,
) -> ThinResult<TermValueRef<'data>> {
    let year_month_is_null = parts.durations.months.is_null(offset);
    let day_time_is_null = parts.durations.seconds.is_null(offset);
    Ok(match (year_month_is_null, day_time_is_null) {
        (false, false) => {
            let mut bytes = [0; 24];
            bytes[0..8].copy_from_slice(&parts.durations.months.value(offset).to_be_bytes());
            bytes[8..24].copy_from_slice(&parts.durations.seconds.value(offset).to_be_bytes());
            TermValueRef::DurationLiteral(Duration::from_be_bytes(bytes))
        }
        (false, true) => TermValueRef::YearMonthDurationLiteral(YearMonthDuration::from_be_bytes(
            parts.durations.months.value(offset).to_be_bytes(),
        )),
        (true, false) => TermValueRef::DayTimeDurationLiteral(DayTimeDuration::from_be_bytes(
            parts.durations.seconds.value(offset).to_be_bytes(),
        )),
        _ => return ThinError::internal_error("Both values are null in a duration."),
    })
}
