use crate::value_encoding::{RdfValueEncoding, RdfValueEncodingField};
use datafusion::arrow::array::{Array, AsArray, UnionArray};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::common::ScalarValue;
use model::{BlankNodeRef, GraphNameRef, NamedNodeRef};
use model::{
    Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int, Integer,
    InternalTermRef, LanguageStringRef, Numeric, SimpleLiteralRef, StringLiteralRef, ThinError,
    ThinResult, Time, Timestamp, TimezoneOffset, TypedLiteralRef, YearMonthDuration,
};
use std::ops::Not;

pub trait FromEncodedTerm<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized;

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self>
    where
        Self: Sized;
}

impl<'data> FromEncodedTerm<'data> for InternalTermRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match scalar {
            ScalarValue::Union(Some((type_id, inner_value)), _, _) => {
                let type_id = RdfValueEncodingField::try_from(*type_id)?;
                Ok(match type_id {
                    RdfValueEncodingField::NamedNode => {
                        InternalTermRef::NamedNode(NamedNodeRef::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::BlankNode => {
                        InternalTermRef::BlankNode(BlankNodeRef::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::String => match inner_value.as_ref() {
                        ScalarValue::Struct(struct_array) => {
                            if struct_array.column(1).is_null(0) {
                                InternalTermRef::SimpleLiteral(SimpleLiteralRef::from_enc_scalar(
                                    scalar,
                                )?)
                            } else {
                                InternalTermRef::LanguageStringLiteral(
                                    LanguageStringRef::from_enc_scalar(scalar)?,
                                )
                            }
                        }
                        _ => return ThinError::expected(),
                    },
                    RdfValueEncodingField::Boolean => {
                        InternalTermRef::BooleanLiteral(Boolean::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::Float
                    | RdfValueEncodingField::Double
                    | RdfValueEncodingField::Decimal
                    | RdfValueEncodingField::Int
                    | RdfValueEncodingField::Integer => {
                        InternalTermRef::NumericLiteral(Numeric::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::Duration => {
                        InternalTermRef::DurationLiteral(Duration::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::DateTime => {
                        InternalTermRef::DateTimeLiteral(DateTime::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::Time => {
                        InternalTermRef::TimeLiteral(Time::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::Date => {
                        InternalTermRef::DateLiteral(Date::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::OtherTypedLiteral => {
                        InternalTermRef::TypedLiteral(TypedLiteralRef::from_enc_scalar(scalar)?)
                    }
                    RdfValueEncodingField::Null => return ThinError::expected(),
                })
            }
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        Ok(match field {
            RdfValueEncodingField::NamedNode => {
                InternalTermRef::NamedNode(NamedNodeRef::from_enc_array(array, index)?)
            }
            RdfValueEncodingField::BlankNode => {
                InternalTermRef::BlankNode(BlankNodeRef::from_enc_array(array, index)?)
            }
            RdfValueEncodingField::String => {
                if array
                    .child(field.type_id())
                    .as_struct()
                    .column(1)
                    .is_null(offset)
                {
                    InternalTermRef::SimpleLiteral(SimpleLiteralRef::from_enc_array(array, index)?)
                } else {
                    InternalTermRef::LanguageStringLiteral(LanguageStringRef::from_enc_array(
                        array, index,
                    )?)
                }
            }
            RdfValueEncodingField::Boolean => {
                InternalTermRef::BooleanLiteral(Boolean::from_enc_array(array, index)?)
            }
            RdfValueEncodingField::Float
            | RdfValueEncodingField::Double
            | RdfValueEncodingField::Decimal
            | RdfValueEncodingField::Int
            | RdfValueEncodingField::Integer => {
                InternalTermRef::NumericLiteral(Numeric::from_enc_array(array, index)?)
            }
            RdfValueEncodingField::DateTime => {
                InternalTermRef::DateTimeLiteral(DateTime::from_enc_array(array, index)?)
            }
            RdfValueEncodingField::Time => {
                InternalTermRef::TimeLiteral(Time::from_enc_array(array, index)?)
            }
            RdfValueEncodingField::Date => {
                InternalTermRef::DateLiteral(Date::from_enc_array(array, index)?)
            }
            RdfValueEncodingField::Duration => {
                let year_month_is_null = array
                    .child(field.type_id())
                    .as_struct()
                    .column(0)
                    .is_null(offset);
                let day_time_is_null = array
                    .child(field.type_id())
                    .as_struct()
                    .column(1)
                    .is_null(offset);
                match (year_month_is_null, day_time_is_null) {
                    (false, false) => {
                        InternalTermRef::DurationLiteral(Duration::from_enc_array(array, index)?)
                    }
                    (false, true) => InternalTermRef::YearMonthDurationLiteral(
                        YearMonthDuration::from_enc_array(array, index)?,
                    ),
                    (true, false) => InternalTermRef::DayTimeDurationLiteral(
                        DayTimeDuration::from_enc_array(array, index)?,
                    ),
                    _ => unreachable!("Unexpected encoding"),
                }
            }
            RdfValueEncodingField::OtherTypedLiteral => {
                InternalTermRef::TypedLiteral(TypedLiteralRef::from_enc_array(array, index)?)
            }
            RdfValueEncodingField::Null => return ThinError::expected(),
        })
    }
}

impl<'data> FromEncodedTerm<'data> for BlankNodeRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::BlankNode.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self::new_unchecked(value.as_str())),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::BlankNode => Ok(BlankNodeRef::new_unchecked(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> FromEncodedTerm<'data> for LanguageStringRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::String.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => {
                if value.column(1).is_null(0) {
                    ThinError::expected()
                } else {
                    Ok(Self {
                        value: value.column(0).as_string::<i32>().value(0),
                        language: value.column(1).as_string::<i32>().value(0),
                    })
                }
            }
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if language.is_null(offset) {
                    return ThinError::expected();
                }

                Ok(Self {
                    value: values.value(offset),
                    language: language.value(offset),
                })
            }
            _ => ThinError::expected(),
        }
    }
}

impl<'data> FromEncodedTerm<'data> for NamedNodeRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::NamedNode.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self::new_unchecked(value.as_str())),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::NamedNode => Ok(NamedNodeRef::new_unchecked(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> FromEncodedTerm<'data> for GraphNameRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        match RdfValueEncodingField::try_from(*type_id)? {
            RdfValueEncodingField::Null => Ok(GraphNameRef::DefaultGraph),
            RdfValueEncodingField::NamedNode => {
                NamedNodeRef::from_enc_scalar(scalar).map(GraphNameRef::NamedNode)
            }
            RdfValueEncodingField::BlankNode => {
                BlankNodeRef::from_enc_scalar(scalar).map(GraphNameRef::BlankNode)
            }
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        match field {
            RdfValueEncodingField::Null => Ok(GraphNameRef::DefaultGraph),
            RdfValueEncodingField::NamedNode => {
                NamedNodeRef::from_enc_array(array, index).map(GraphNameRef::NamedNode)
            }
            RdfValueEncodingField::BlankNode => {
                BlankNodeRef::from_enc_array(array, index).map(GraphNameRef::BlankNode)
            }
            _ => ThinError::expected(),
        }
    }
}

impl<'data> FromEncodedTerm<'data> for SimpleLiteralRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::String.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => {
                if value.column(1).is_null(0) {
                    Ok(Self {
                        value: value.column(0).as_string::<i32>().value(0),
                    })
                } else {
                    ThinError::expected()
                }
            }
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if !language.is_null(offset) {
                    return ThinError::expected();
                }

                Ok(Self {
                    value: values.value(offset),
                })
            }
            _ => ThinError::expected(),
        }
    }
}
impl<'data> FromEncodedTerm<'data> for StringLiteralRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::String.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Struct(array) => {
                let value_arr = array.column(0).as_string::<i32>();
                let language_arr = array.column(1).as_string::<i32>();
                if language_arr.is_null(0) {
                    Ok(Self(value_arr.value(0), None))
                } else {
                    Ok(Self(value_arr.value(0), Some(language_arr.value(0))))
                }
            }
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::String => {
                let array = array.child(field.type_id()).as_struct();
                let value_arr = array.column(0).as_string::<i32>();
                let language_arr = array.column(1).as_string::<i32>();
                if language_arr.is_null(offset) {
                    Ok(Self(value_arr.value(offset), None))
                } else {
                    Ok(Self(
                        value_arr.value(offset),
                        Some(language_arr.value(offset)),
                    ))
                }
            }
            _ => ThinError::expected(),
        }
    }
}

impl<'data> FromEncodedTerm<'data> for TypedLiteralRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::OtherTypedLiteral.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => {
                if value.column(1).is_null(0) {
                    ThinError::expected()
                } else {
                    Ok(Self {
                        value: value.column(0).as_string::<i32>().value(0),
                        literal_type: value.column(1).as_string::<i32>().value(0),
                    })
                }
            }
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::OtherTypedLiteral => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let datatypes = array.column(1).as_string::<i32>();
                Ok(Self {
                    value: values.value(offset),
                    literal_type: datatypes.value(offset),
                })
            }
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for Boolean {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::Boolean.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Boolean(Some(value)) => Ok(Self::from(*value)),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::Boolean => Ok(array
                .child(field.type_id())
                .as_boolean()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for Decimal {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::Decimal.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Decimal128(Some(value), _, _) => {
                Ok(Decimal::from_be_bytes(value.to_be_bytes()))
            }
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::Decimal => Ok(Decimal::from_be_bytes(
                array
                    .child(field.type_id())
                    .as_primitive::<Decimal128Type>()
                    .value(offset)
                    .to_be_bytes(),
            )),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for Double {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::Double.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Float64(Some(value)) => Ok((*value).into()),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::Double => Ok(array
                .child(field.type_id())
                .as_primitive::<Float64Type>()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for Float {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::Float.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Float32(Some(value)) => Ok((*value).into()),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::Float => Ok(array
                .child(field.type_id())
                .as_primitive::<Float32Type>()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for Int {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::Int.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok(Self::new(*value)),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::Int => Ok(array
                .child(field.type_id())
                .as_primitive::<Int32Type>()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for Integer {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::Int.type_id()
            && *type_id != RdfValueEncodingField::Integer.type_id()
        {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok((*value).into()),
            ScalarValue::Int64(Some(value)) => Ok((*value).into()),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            RdfValueEncodingField::Int => Ok(array
                .child(field.type_id())
                .as_primitive::<Int32Type>()
                .value(offset)
                .into()),
            RdfValueEncodingField::Integer => Ok(array
                .child(field.type_id())
                .as_primitive::<Int64Type>()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for Numeric {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        let field = RdfValueEncodingField::try_from(*type_id)?;
        match field {
            RdfValueEncodingField::Int => Ok(Self::Int(Int::from_enc_scalar(scalar)?)),
            RdfValueEncodingField::Integer => Ok(Self::Integer(Integer::from_enc_scalar(scalar)?)),
            RdfValueEncodingField::Float => Ok(Self::Float(Float::from_enc_scalar(scalar)?)),
            RdfValueEncodingField::Double => Ok(Self::Double(Double::from_enc_scalar(scalar)?)),
            RdfValueEncodingField::Decimal => Ok(Self::Decimal(Decimal::from_enc_scalar(scalar)?)),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        match field {
            RdfValueEncodingField::Int => Ok(Self::Int(Int::from_enc_array(array, index)?)),
            RdfValueEncodingField::Integer => {
                Ok(Self::Integer(Integer::from_enc_array(array, index)?))
            }
            RdfValueEncodingField::Float => Ok(Self::Float(Float::from_enc_array(array, index)?)),
            RdfValueEncodingField::Double => {
                Ok(Self::Double(Double::from_enc_array(array, index)?))
            }
            RdfValueEncodingField::Decimal => {
                Ok(Self::Decimal(Decimal::from_enc_array(array, index)?))
            }
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for Duration {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (Some(year_month), Some(day_time)) => Ok(Duration::new(year_month, day_time)?),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (Some(year_month), Some(day_time)) => Ok(Duration::new(year_month, day_time)?),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for YearMonthDuration {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (Some(year_month), None) => Ok(YearMonthDuration::new(year_month)),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (Some(year_month), None) => Ok(YearMonthDuration::new(year_month)),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for DayTimeDuration {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (None, Some(day_time)) => Ok(DayTimeDuration::new(day_time)),
            _ => ThinError::expected(),
        }
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (None, Some(day_time)) => Ok(DayTimeDuration::new(day_time)),
            _ => ThinError::expected(),
        }
    }
}

impl FromEncodedTerm<'_> for DateTime {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::DateTime.type_id() {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_enc_scalar(scalar)?;
        Ok(DateTime::new(timestamp))
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        if field != RdfValueEncodingField::DateTime {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_enc_array(array, index)?;
        Ok(DateTime::new(timestamp))
    }
}

impl FromEncodedTerm<'_> for Time {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::Time.type_id() {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_enc_scalar(scalar)?;
        Ok(Time::new(timestamp))
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        if field != RdfValueEncodingField::Time {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_enc_array(array, index)?;
        Ok(Time::new(timestamp))
    }
}

impl FromEncodedTerm<'_> for Date {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != RdfValueEncodingField::Date.type_id() {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_enc_scalar(scalar)?;
        Ok(Date::new(timestamp))
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        if field != RdfValueEncodingField::Date {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_enc_array(array, index)?;
        Ok(Date::new(timestamp))
    }
}

impl FromEncodedTerm<'_> for Timestamp {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((_, value)), _, _) = scalar else {
            return ThinError::expected();
        };

        if value.data_type() != DataType::Struct(RdfValueEncoding::timestamp_fields()) {
            return ThinError::expected();
        }

        let ScalarValue::Struct(struct_array) = value.as_ref() else {
            unreachable!("Type already checked.")
        };

        let value = struct_array.column(0).as_primitive::<Decimal128Type>();
        let offset = struct_array.column(1).as_primitive::<Int16Type>();

        Ok(Timestamp::new(
            Decimal::from_be_bytes(value.value(0).to_be_bytes()),
            offset
                .is_null(0)
                .not()
                .then(|| TimezoneOffset::new_unchecked(offset.value(0))),
        ))
    }

    fn from_enc_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let offset = array.value_offset(index);
        let field = RdfValueEncodingField::try_from(array.type_id(index))?;
        let struct_array = array
            .child(field.type_id())
            .as_struct_opt()
            .ok_or(ThinError::Expected)?;

        let value_array = struct_array.column(0).as_primitive::<Decimal128Type>();
        let offset_array = struct_array.column(1).as_primitive::<Int16Type>();

        Ok(Timestamp::new(
            Decimal::from_be_bytes(value_array.value(offset).to_be_bytes()),
            offset_array
                .is_null(offset)
                .not()
                .then(|| TimezoneOffset::new_unchecked(offset_array.value(offset))),
        ))
    }
}

fn get_duration_encoding_scalar(
    scalar: &ScalarValue,
) -> ThinResult<(Option<i64>, Option<Decimal>)> {
    let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
        return ThinError::expected();
    };

    let field = RdfValueEncodingField::try_from(*type_id)?;
    if field != RdfValueEncodingField::Duration {
        return ThinError::expected();
    }

    let ScalarValue::Struct(struct_array) = scalar.as_ref() else {
        return ThinError::expected();
    };

    let year_month_array = struct_array.as_ref().column(0).as_primitive::<Int64Type>();
    let day_time_array = struct_array
        .as_ref()
        .column(0)
        .as_primitive::<Decimal128Type>();

    let year_month = year_month_array
        .is_null(0)
        .not()
        .then(|| year_month_array.value(0));
    let day_time = day_time_array
        .is_null(0)
        .not()
        .then(|| Decimal::from_be_bytes(day_time_array.value(0).to_be_bytes()));
    Ok((year_month, day_time))
}

fn get_duration_encoding_array(
    array: &'_ UnionArray,
    index: usize,
) -> ThinResult<(Option<i64>, Option<Decimal>)> {
    let field = RdfValueEncodingField::try_from(array.type_id(index))?;
    if field != RdfValueEncodingField::Duration {
        return ThinError::expected();
    }

    let offset = array.value_offset(index);
    let struct_array = array.child(field.type_id()).as_struct();
    let year_month_array = struct_array.column(0).as_primitive::<Int64Type>();
    let day_time_array = struct_array.column(1).as_primitive::<Decimal128Type>();

    let year_month = year_month_array
        .is_null(offset)
        .not()
        .then(|| year_month_array.value(offset));
    let day_time = day_time_array
        .is_null(offset)
        .not()
        .then(|| Decimal::from_be_bytes(day_time_array.value(offset).to_be_bytes()));
    Ok((year_month, day_time))
}
