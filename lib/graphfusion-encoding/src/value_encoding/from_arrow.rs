use crate::value_encoding::{TermValueEncoding, ValueEncodingField};
use crate::FromArrow;
use datafusion::arrow::array::{Array, AsArray, UnionArray};
use datafusion::arrow::datatypes::{
    DataType, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
};
use datafusion::common::ScalarValue;
use model::{BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef};
use model::{
    Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int, Integer,
    LanguageStringRef, Numeric, TermValueRef, SimpleLiteralRef, StringLiteralRef, ThinError,
    ThinResult, Time, Timestamp, TimezoneOffset, YearMonthDuration,
};
use std::ops::Not;

impl<'data> FromArrow<'data> for TermValueRef<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match scalar {
            ScalarValue::Union(Some((type_id, inner_value)), _, _) => {
                let type_id = ValueEncodingField::try_from(*type_id)?;
                Ok(match type_id {
                    ValueEncodingField::NamedNode => {
                        TermValueRef::NamedNode(NamedNodeRef::from_scalar(scalar)?)
                    }
                    ValueEncodingField::BlankNode => {
                        TermValueRef::BlankNode(BlankNodeRef::from_scalar(scalar)?)
                    }
                    ValueEncodingField::String => match inner_value.as_ref() {
                        ScalarValue::Struct(struct_array) => {
                            if struct_array.column(1).is_null(0) {
                                TermValueRef::SimpleLiteral(SimpleLiteralRef::from_scalar(
                                    scalar,
                                )?)
                            } else {
                                TermValueRef::LanguageStringLiteral(
                                    LanguageStringRef::from_scalar(scalar)?,
                                )
                            }
                        }
                        _ => return ThinError::expected(),
                    },
                    ValueEncodingField::Boolean => {
                        TermValueRef::BooleanLiteral(Boolean::from_scalar(scalar)?)
                    }
                    ValueEncodingField::Float
                    | ValueEncodingField::Double
                    | ValueEncodingField::Decimal
                    | ValueEncodingField::Int
                    | ValueEncodingField::Integer => {
                        TermValueRef::NumericLiteral(Numeric::from_scalar(scalar)?)
                    }
                    ValueEncodingField::Duration => {
                        TermValueRef::DurationLiteral(Duration::from_scalar(scalar)?)
                    }
                    ValueEncodingField::DateTime => {
                        TermValueRef::DateTimeLiteral(DateTime::from_scalar(scalar)?)
                    }
                    ValueEncodingField::Time => {
                        TermValueRef::TimeLiteral(Time::from_scalar(scalar)?)
                    }
                    ValueEncodingField::Date => {
                        TermValueRef::DateLiteral(Date::from_scalar(scalar)?)
                    }
                    ValueEncodingField::OtherLiteral => {
                        TermValueRef::OtherLiteral(LiteralRef::from_scalar(scalar)?)
                    }
                    ValueEncodingField::Null => return ThinError::expected(),
                })
            }
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        Ok(match field {
            ValueEncodingField::NamedNode => {
                TermValueRef::NamedNode(NamedNodeRef::from_array(array, index)?)
            }
            ValueEncodingField::BlankNode => {
                TermValueRef::BlankNode(BlankNodeRef::from_array(array, index)?)
            }
            ValueEncodingField::String => {
                if array
                    .child(field.type_id())
                    .as_struct()
                    .column(1)
                    .is_null(offset)
                {
                    TermValueRef::SimpleLiteral(SimpleLiteralRef::from_array(array, index)?)
                } else {
                    TermValueRef::LanguageStringLiteral(LanguageStringRef::from_array(
                        array, index,
                    )?)
                }
            }
            ValueEncodingField::Boolean => {
                TermValueRef::BooleanLiteral(Boolean::from_array(array, index)?)
            }
            ValueEncodingField::Float
            | ValueEncodingField::Double
            | ValueEncodingField::Decimal
            | ValueEncodingField::Int
            | ValueEncodingField::Integer => {
                TermValueRef::NumericLiteral(Numeric::from_array(array, index)?)
            }
            ValueEncodingField::DateTime => {
                TermValueRef::DateTimeLiteral(DateTime::from_array(array, index)?)
            }
            ValueEncodingField::Time => {
                TermValueRef::TimeLiteral(Time::from_array(array, index)?)
            }
            ValueEncodingField::Date => {
                TermValueRef::DateLiteral(Date::from_array(array, index)?)
            }
            ValueEncodingField::Duration => {
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
                        TermValueRef::DurationLiteral(Duration::from_array(array, index)?)
                    }
                    (false, true) => TermValueRef::YearMonthDurationLiteral(
                        YearMonthDuration::from_array(array, index)?,
                    ),
                    (true, false) => TermValueRef::DayTimeDurationLiteral(
                        DayTimeDuration::from_array(array, index)?,
                    ),
                    _ => unreachable!("Unexpected encoding"),
                }
            }
            ValueEncodingField::OtherLiteral => {
                TermValueRef::OtherLiteral(LiteralRef::from_array(array, index)?)
            }
            ValueEncodingField::Null => return ThinError::expected(),
        })
    }
}

impl<'data> FromArrow<'data> for BlankNodeRef<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::BlankNode.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self::new_unchecked(value.as_str())),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::BlankNode => Ok(BlankNodeRef::new_unchecked(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> FromArrow<'data> for LanguageStringRef<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::String.type_id() {
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

    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::String => {
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

impl<'data> FromArrow<'data> for NamedNodeRef<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::NamedNode.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self::new_unchecked(value.as_str())),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::NamedNode => Ok(NamedNodeRef::new_unchecked(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => ThinError::expected(),
        }
    }
}

impl<'data> FromArrow<'data> for GraphNameRef<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        match ValueEncodingField::try_from(*type_id)? {
            ValueEncodingField::Null => Ok(GraphNameRef::DefaultGraph),
            ValueEncodingField::NamedNode => {
                NamedNodeRef::from_scalar(scalar).map(GraphNameRef::NamedNode)
            }
            ValueEncodingField::BlankNode => {
                BlankNodeRef::from_scalar(scalar).map(GraphNameRef::BlankNode)
            }
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        match field {
            ValueEncodingField::Null => Ok(GraphNameRef::DefaultGraph),
            ValueEncodingField::NamedNode => {
                NamedNodeRef::from_array(array, index).map(GraphNameRef::NamedNode)
            }
            ValueEncodingField::BlankNode => {
                BlankNodeRef::from_array(array, index).map(GraphNameRef::BlankNode)
            }
            _ => ThinError::expected(),
        }
    }
}

impl<'data> FromArrow<'data> for SimpleLiteralRef<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::String.type_id() {
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

    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::String => {
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
impl<'data> FromArrow<'data> for StringLiteralRef<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::String.type_id() {
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

    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::String => {
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

impl<'data> FromArrow<'data> for LiteralRef<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::OtherLiteral.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Struct(struct_array) => {
                if struct_array.column(1).is_null(0) {
                    ThinError::expected()
                } else {
                    let value = struct_array.column(0).as_string::<i32>().value(0);
                    let datatype = struct_array.column(1).as_string::<i32>().value(0);
                    let datatype = NamedNodeRef::new_unchecked(datatype);
                    Ok(LiteralRef::new_typed_literal(value, datatype))
                }
            }
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::OtherLiteral => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let datatypes = array.column(1).as_string::<i32>();
                let datatype = NamedNodeRef::new_unchecked(datatypes.value(offset));
                Ok(Self::new_typed_literal(values.value(offset), datatype))
            }
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for Boolean {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::Boolean.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Boolean(Some(value)) => Ok(Self::from(*value)),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::Boolean => Ok(array
                .child(field.type_id())
                .as_boolean()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for Decimal {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::Decimal.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Decimal128(Some(value), _, _) => {
                Ok(Decimal::from_be_bytes(value.to_be_bytes()))
            }
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::Decimal => Ok(Decimal::from_be_bytes(
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

impl FromArrow<'_> for Double {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::Double.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Float64(Some(value)) => Ok((*value).into()),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::Double => Ok(array
                .child(field.type_id())
                .as_primitive::<Float64Type>()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for Float {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::Float.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Float32(Some(value)) => Ok((*value).into()),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::Float => Ok(array
                .child(field.type_id())
                .as_primitive::<Float32Type>()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for Int {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::Int.type_id() {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok(Self::new(*value)),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::Int => Ok(array
                .child(field.type_id())
                .as_primitive::<Int32Type>()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for Integer {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::Int.type_id()
            && *type_id != ValueEncodingField::Integer.type_id()
        {
            return ThinError::expected();
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok((*value).into()),
            ScalarValue::Int64(Some(value)) => Ok((*value).into()),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            ValueEncodingField::Int => Ok(array
                .child(field.type_id())
                .as_primitive::<Int32Type>()
                .value(offset)
                .into()),
            ValueEncodingField::Integer => Ok(array
                .child(field.type_id())
                .as_primitive::<Int64Type>()
                .value(offset)
                .into()),
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for Numeric {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        let field = ValueEncodingField::try_from(*type_id)?;
        match field {
            ValueEncodingField::Int => Ok(Self::Int(Int::from_scalar(scalar)?)),
            ValueEncodingField::Integer => Ok(Self::Integer(Integer::from_scalar(scalar)?)),
            ValueEncodingField::Float => Ok(Self::Float(Float::from_scalar(scalar)?)),
            ValueEncodingField::Double => Ok(Self::Double(Double::from_scalar(scalar)?)),
            ValueEncodingField::Decimal => Ok(Self::Decimal(Decimal::from_scalar(scalar)?)),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        match field {
            ValueEncodingField::Int => Ok(Self::Int(Int::from_array(array, index)?)),
            ValueEncodingField::Integer => {
                Ok(Self::Integer(Integer::from_array(array, index)?))
            }
            ValueEncodingField::Float => Ok(Self::Float(Float::from_array(array, index)?)),
            ValueEncodingField::Double => {
                Ok(Self::Double(Double::from_array(array, index)?))
            }
            ValueEncodingField::Decimal => {
                Ok(Self::Decimal(Decimal::from_array(array, index)?))
            }
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for Duration {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (Some(year_month), Some(day_time)) => Ok(Duration::new(year_month, day_time)?),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (Some(year_month), Some(day_time)) => Ok(Duration::new(year_month, day_time)?),
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for YearMonthDuration {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (Some(year_month), None) => Ok(YearMonthDuration::new(year_month)),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (Some(year_month), None) => Ok(YearMonthDuration::new(year_month)),
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for DayTimeDuration {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (None, Some(day_time)) => Ok(DayTimeDuration::new(day_time)),
            _ => ThinError::expected(),
        }
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (None, Some(day_time)) => Ok(DayTimeDuration::new(day_time)),
            _ => ThinError::expected(),
        }
    }
}

impl FromArrow<'_> for DateTime {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::DateTime.type_id() {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_scalar(scalar)?;
        Ok(DateTime::new(timestamp))
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        if field != ValueEncodingField::DateTime {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_array(array, index)?;
        Ok(DateTime::new(timestamp))
    }
}

impl FromArrow<'_> for Time {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::Time.type_id() {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_scalar(scalar)?;
        Ok(Time::new(timestamp))
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        if field != ValueEncodingField::Time {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_array(array, index)?;
        Ok(Time::new(timestamp))
    }
}

impl FromArrow<'_> for Date {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return ThinError::expected();
        };

        if *type_id != ValueEncodingField::Date.type_id() {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_scalar(scalar)?;
        Ok(Date::new(timestamp))
    }

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let field = ValueEncodingField::try_from(array.type_id(index))?;
        if field != ValueEncodingField::Date {
            return ThinError::expected();
        }

        let timestamp = Timestamp::from_array(array, index)?;
        Ok(Date::new(timestamp))
    }
}

impl FromArrow<'_> for Timestamp {
    fn from_scalar(scalar: &'_ ScalarValue) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((_, value)), _, _) = scalar else {
            return ThinError::expected();
        };

        if value.data_type() != DataType::Struct(TermValueEncoding::timestamp_fields()) {
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

    fn from_array(array: &'_ UnionArray, index: usize) -> ThinResult<Self> {
        let offset = array.value_offset(index);
        let field = ValueEncodingField::try_from(array.type_id(index))?;
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

    let field = ValueEncodingField::try_from(*type_id)?;
    if field != ValueEncodingField::Duration {
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
    let field = ValueEncodingField::try_from(array.type_id(index))?;
    if field != ValueEncodingField::Duration {
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
