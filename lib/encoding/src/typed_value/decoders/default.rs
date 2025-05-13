use crate::encoding::{EncodingArray, TermDecoder};
use crate::typed_value::array::{DurationParts, StringParts, TermValueArrayParts, TimestampParts};
use crate::typed_value::{TypedValueEncoding, TypedValueEncodingField};
use crate::{EncodingScalar, TermEncoding};
use datafusion::arrow::array::{Array, AsArray};
use datafusion::common::ScalarValue;
use rdf_fusion_model::{
    BlankNodeRef, Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int,
    Integer, LanguageStringRef, LiteralRef, NamedNodeRef, Numeric, SimpleLiteralRef, ThinError,
    ThinResult, Time, Timestamp, TimezoneOffset, TypedValueRef, YearMonthDuration,
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
        let ScalarValue::Union(Some((type_id, value)), _, _) = scalar.scalar_value() else {
            return ThinError::internal_error("Unexpected type id");
        };

        let field = TypedValueEncodingField::try_from(*type_id)
            .map_err(|_| ThinError::InternalError("Unexpected type id"))?;
        let result = match (field, value.as_ref()) {
            (TypedValueEncodingField::NamedNode, ScalarValue::Utf8(Some(value))) => {
                TypedValueRef::NamedNode(NamedNodeRef::new_unchecked(value))
            }
            (TypedValueEncodingField::BlankNode, ScalarValue::Utf8(Some(value))) => {
                TypedValueRef::BlankNode(BlankNodeRef::new_unchecked(value))
            }
            (TypedValueEncodingField::String, ScalarValue::Struct(struct_array)) => {
                let parts = StringParts {
                    value: struct_array.column(0).as_string::<i32>(),
                    language: struct_array.column(1).as_string::<i32>(),
                };
                extract_string(parts, 0)
            }
            (TypedValueEncodingField::Boolean, ScalarValue::Boolean(Some(value))) => {
                TypedValueRef::BooleanLiteral((*value).into())
            }
            (TypedValueEncodingField::Float, ScalarValue::Float32(Some(value))) => {
                TypedValueRef::NumericLiteral(Numeric::Float((*value).into()))
            }
            (TypedValueEncodingField::Double, ScalarValue::Float64(Some(value))) => {
                TypedValueRef::NumericLiteral(Numeric::Double((*value).into()))
            }
            (TypedValueEncodingField::Decimal, ScalarValue::Decimal128(Some(value), _, _)) => {
                TypedValueRef::NumericLiteral(Numeric::Decimal(Decimal::from_be_bytes(
                    value.to_be_bytes(),
                )))
            }
            (TypedValueEncodingField::Int, ScalarValue::Int32(Some(value))) => {
                TypedValueRef::NumericLiteral(Numeric::Int((*value).into()))
            }
            (TypedValueEncodingField::Integer, ScalarValue::Int64(Some(value))) => {
                TypedValueRef::NumericLiteral(Numeric::Integer((*value).into()))
            }
            (TypedValueEncodingField::Duration, ScalarValue::Struct(struct_array)) => {
                let parts = DurationParts {
                    months: struct_array.column(0).as_primitive(),
                    seconds: struct_array.column(1).as_primitive(),
                };
                extract_duration(parts, 0)?
            }
            (TypedValueEncodingField::DateTime, ScalarValue::Struct(struct_array)) => {
                let parts = TimestampParts {
                    value: struct_array.column(0).as_primitive(),
                    offset: struct_array.column(1).as_primitive(),
                };
                let timestamp = extract_timestamp(parts, 0);
                TypedValueRef::DateTimeLiteral(DateTime::new(timestamp))
            }
            (TypedValueEncodingField::Time, ScalarValue::Struct(struct_array)) => {
                let parts = TimestampParts {
                    value: struct_array.column(0).as_primitive(),
                    offset: struct_array.column(1).as_primitive(),
                };
                let timestamp = extract_timestamp(parts, 0);
                TypedValueRef::TimeLiteral(Time::new(timestamp))
            }
            (TypedValueEncodingField::Date, ScalarValue::Struct(struct_array)) => {
                let parts = TimestampParts {
                    value: struct_array.column(0).as_primitive(),
                    offset: struct_array.column(1).as_primitive(),
                };
                let timestamp = extract_timestamp(parts, 0);
                TypedValueRef::DateLiteral(Date::new(timestamp))
            }
            (TypedValueEncodingField::OtherLiteral, ScalarValue::Struct(struct_array)) => {
                let value = struct_array.column(0).as_string::<i32>().value(0);
                let datatype = struct_array.column(1).as_string::<i32>().value(0);
                let datatype = NamedNodeRef::new_unchecked(datatype);
                TypedValueRef::OtherLiteral(LiteralRef::new_typed_literal(value, datatype))
            }
            (TypedValueEncodingField::Null, _) => return ThinError::expected(),
            _ => return ThinError::internal_error("Unexpected type id / value combination"),
        };
        Ok(result)
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
        TypedValueEncodingField::String => Ok(extract_string(parts.strings, offset)),
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
        TypedValueEncodingField::Duration => extract_duration(parts.durations, offset),
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

/// TODO
fn extract_string(parts: StringParts<'_>, offset: usize) -> TypedValueRef<'_> {
    if parts.language.is_null(offset) {
        TypedValueRef::SimpleLiteral(SimpleLiteralRef::new(parts.value.value(offset)))
    } else {
        TypedValueRef::LanguageStringLiteral(LanguageStringRef::new(
            parts.value.value(offset),
            parts.language.value(offset),
        ))
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

fn extract_duration(parts: DurationParts<'_>, offset: usize) -> ThinResult<TypedValueRef<'_>> {
    let year_month_is_null = parts.months.is_null(offset);
    let day_time_is_null = parts.seconds.is_null(offset);
    Ok(match (year_month_is_null, day_time_is_null) {
        (false, false) => {
            let mut bytes = [0; 24];
            bytes[0..8].copy_from_slice(&parts.months.value(offset).to_be_bytes());
            bytes[8..24].copy_from_slice(&parts.seconds.value(offset).to_be_bytes());
            TypedValueRef::DurationLiteral(Duration::from_be_bytes(bytes))
        }
        (false, true) => TypedValueRef::YearMonthDurationLiteral(YearMonthDuration::from_be_bytes(
            parts.months.value(offset).to_be_bytes(),
        )),
        (true, false) => TypedValueRef::DayTimeDurationLiteral(DayTimeDuration::from_be_bytes(
            parts.seconds.value(offset).to_be_bytes(),
        )),
        _ => return ThinError::internal_error("Both values are null in a duration."),
    })
}
