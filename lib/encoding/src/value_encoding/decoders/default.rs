use crate::encoding::{EncodingArray, TermDecoder};
use crate::value_encoding::array::{TermValueArrayParts, TimestampParts};
use crate::value_encoding::{TypedValueEncoding, TypedValueEncodingField};
use crate::TermEncoding;
use datafusion::arrow::array::Array;
use graphfusion_model::{
    BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int,
    Integer, LanguageStringRef, LiteralRef, NamedNodeRef, Numeric, SimpleLiteralRef, TypedValueRef,
    ThinError, ThinResult, Time, Timestamp, TimezoneOffset, YearMonthDuration,
};
use std::ops::Not;

/// TODO
/// Extracts a sequence of term references from the given array.
#[derive(Debug)]
pub struct DefaultTypedValueDecoder;

/// Extracts a sequence of term references from the given array.
impl TermDecoder<TypedValueEncoding> for DefaultTypedValueDecoder {
    type Term<'data> = TypedValueRef<'data>;

    fn decode_terms(
        array: &<TypedValueEncoding as TermEncoding>::Array,
    ) -> impl Iterator<Item = ThinResult<Self::Term<'_>>> {
        let parts = array.parts_as_ref();
        (0..array.array().len()).map(move |idx| extract_term_value(&parts, idx))
    }

    fn decode_term(
        scalar: &<TypedValueEncoding as TermEncoding>::Scalar,
    ) -> ThinResult<Self::Term<'_>> {
        todo!()
    }
}

fn extract_term_value<'data>(
    parts: &TermValueArrayParts<'data>,
    idx: usize,
) -> ThinResult<TypedValueRef<'data>> {
    let field = TypedValueEncodingField::try_from(parts.array.type_id(idx))
        .map_err(|_| ThinError::InternalError("Unexpected type id"))?;
    let offset = parts.array.value_offset(idx);

    match field {
        TypedValueEncodingField::Null => ThinError::expected(),
        TypedValueEncodingField::NamedNode => {
            let value = NamedNodeRef::new_unchecked(parts.named_nodes.value(offset));
            Ok(TypedValueRef::NamedNode(value))
        }
        TypedValueEncodingField::BlankNode => {
            let value = BlankNodeRef::new_unchecked(parts.blank_nodes.value(offset));
            Ok(TypedValueRef::BlankNode(value))
        }
        TypedValueEncodingField::String => {
            if parts.strings.language.is_null(offset) {
                Ok(TypedValueRef::SimpleLiteral(SimpleLiteralRef::new(
                    parts.strings.value.value(offset),
                )))
            } else {
                Ok(TypedValueRef::LanguageStringLiteral(LanguageStringRef::new(
                    parts.strings.value.value(offset),
                    parts.strings.language.value(offset),
                )))
            }
        }
        TypedValueEncodingField::Boolean => {
            let value = Boolean::from(parts.booleans.value(offset));
            Ok(TypedValueRef::BooleanLiteral(value))
        }
        TypedValueEncodingField::Float => {
            let value = Float::from(parts.floats.value(offset));
            Ok(TypedValueRef::NumericLiteral(Numeric::Float(value)))
        }
        TypedValueEncodingField::Double => {
            let value = Double::from(parts.doubles.value(offset));
            Ok(TypedValueRef::NumericLiteral(Numeric::Double(value)))
        }
        TypedValueEncodingField::Decimal => {
            let value = Decimal::from_be_bytes(parts.decimals.value(offset).to_be_bytes());
            Ok(TypedValueRef::NumericLiteral(Numeric::Decimal(value)))
        }
        TypedValueEncodingField::Int => {
            let value = Int::from(parts.ints.value(offset));
            Ok(TypedValueRef::NumericLiteral(Numeric::Int(value)))
        }
        TypedValueEncodingField::Integer => {
            let value = Integer::from(parts.integers.value(offset));
            Ok(TypedValueRef::NumericLiteral(Numeric::Integer(value)))
        }
        TypedValueEncodingField::DateTime => {
            let timestamp = extract_timestamp(parts.date_times, offset);
            Ok(TypedValueRef::DateTimeLiteral(DateTime::new(timestamp)))
        }
        TypedValueEncodingField::Time => {
            let timestamp = extract_timestamp(parts.times, offset);
            Ok(TypedValueRef::TimeLiteral(Time::new(timestamp)))
        }
        TypedValueEncodingField::Date => {
            let timestamp = extract_timestamp(parts.dates, offset);
            Ok(TypedValueRef::DateLiteral(Date::new(timestamp)))
        }
        TypedValueEncodingField::Duration => extract_duration(parts, offset),
        TypedValueEncodingField::OtherLiteral => {
            let value = parts.other_literals.value.value(offset);
            let datatype = parts.other_literals.datatype.value(offset);
            let datatype = NamedNodeRef::new_unchecked(datatype);
            Ok(TypedValueRef::OtherLiteral(LiteralRef::new_typed_literal(
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
) -> ThinResult<TypedValueRef<'data>> {
    let year_month_is_null = parts.durations.months.is_null(offset);
    let day_time_is_null = parts.durations.seconds.is_null(offset);
    Ok(match (year_month_is_null, day_time_is_null) {
        (false, false) => {
            let mut bytes = [0; 24];
            bytes[0..8].copy_from_slice(&parts.durations.months.value(offset).to_be_bytes());
            bytes[8..24].copy_from_slice(&parts.durations.seconds.value(offset).to_be_bytes());
            TypedValueRef::DurationLiteral(Duration::from_be_bytes(bytes))
        }
        (false, true) => TypedValueRef::YearMonthDurationLiteral(YearMonthDuration::from_be_bytes(
            parts.durations.months.value(offset).to_be_bytes(),
        )),
        (true, false) => TypedValueRef::DayTimeDurationLiteral(DayTimeDuration::from_be_bytes(
            parts.durations.seconds.value(offset).to_be_bytes(),
        )),
        _ => return ThinError::internal_error("Both values are null in a duration."),
    })
}
