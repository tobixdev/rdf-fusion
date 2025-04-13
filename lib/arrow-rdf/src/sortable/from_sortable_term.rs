use crate::encoded::{EncTerm, EncTermField};
use datafusion::arrow::array::{Array, AsArray, UnionArray};
use datafusion::arrow::datatypes::{DataType, Decimal128Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type};
use datafusion::common::ScalarValue;
use datamodel::{Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int, Integer, LanguageStringRef, Numeric, RdfOpResult, SimpleLiteralRef, StringLiteralRef, TermRef, Time, Timestamp, TimezoneOffset, TypedLiteralRef, YearMonthDuration};
use oxrdf::{BlankNodeRef, NamedNodeRef};
use std::ops::Not;

pub trait FromSortableTerm<'data> {
    fn from_sortable_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self>
    where
        Self: Sized;
}

impl<'data> FromSortableTerm<'data> for TermRef<'data> {
    fn from_sortable_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        Ok(match field {
            EncTermField::NamedNode => TermRef::NamedNode(
                NamedNodeRef::from_sortable_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::BlankNode => TermRef::BlankNode(
                BlankNodeRef::from_sortable_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::String => match array
                .child(field.type_id())
                .as_struct()
                .column(1)
                .is_null(offset)
            {
                true => TermRef::SimpleLiteral(
                    SimpleLiteralRef::from_sortable_array(array, index)
                        .expect("EncTermField and null checked"),
                ),
                false => TermRef::LanguageStringLiteral(
                    LanguageStringRef::from_sortable_array(array, index)
                        .expect("EncTermField and null checked"),
                ),
            },
            EncTermField::Boolean => TermRef::BooleanLiteral(
                Boolean::from_sortable_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Float
            | EncTermField::Double
            | EncTermField::Decimal
            | EncTermField::Int
            | EncTermField::Integer => TermRef::NumericLiteral(
                Numeric::from_sortable_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::DateTime => TermRef::DateTimeLiteral(
                DateTime::from_sortable_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Time => TermRef::TimeLiteral(
                Time::from_sortable_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Date => TermRef::DateLiteral(
                Date::from_sortable_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Duration => {
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
                    (false, false) => TermRef::DurationLiteral(Duration::from_sortable_array(array, index)?),
                    (false, true) => TermRef::YearMonthDurationLiteral(YearMonthDuration::from_sortable_array(array, index)?),
                    (true, false) => TermRef::DayTimeDurationLiteral(DayTimeDuration::from_sortable_array(array, index)?),
                    _ => unreachable!("Unexpected encoding"),
                }
            },
            EncTermField::TypedLiteral => TermRef::TypedLiteral(
                TypedLiteralRef::from_sortable_array(array, index).expect("EncTermField checked"),
            ),
            EncTermField::Null => Err(())?,
        })
    }
}

impl<'data> FromSortableTerm<'data> for BlankNodeRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::BlankNode.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self::new_unchecked(value.as_str())),
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::BlankNode => Ok(BlankNodeRef::new_unchecked(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => Err(()),
        }
    }
}

impl<'data> FromSortableTerm<'data> for LanguageStringRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::String.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => Err(()),
                false => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                    language: value.column(1).as_string::<i32>().value(0),
                }),
            },
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if language.is_null(offset) {
                    return Err(());
                }

                Ok(Self {
                    value: values.value(offset),
                    language: language.value(offset),
                })
            }
            _ => Err(()),
        }
    }
}

impl<'data> FromSortableTerm<'data> for NamedNodeRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::NamedNode.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self::new_unchecked(value.as_str())),
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::NamedNode => Ok(NamedNodeRef::new_unchecked(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => Err(()),
        }
    }
}

impl<'data> FromSortableTerm<'data> for SimpleLiteralRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::String.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                }),
                false => Err(()),
            },
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if !language.is_null(offset) {
                    return Err(());
                }

                Ok(Self {
                    value: values.value(offset),
                })
            }
            _ => Err(()),
        }
    }
}
impl<'data> FromSortableTerm<'data> for StringLiteralRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::String.type_id() {
            return Err(());
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
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::String => {
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
            _ => Err(()),
        }
    }
}

impl<'data> FromSortableTerm<'data> for TypedLiteralRef<'data> {
    fn from_enc_scalar(scalar: &'data ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::TypedLiteral.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => Err(()),
                false => Ok(Self {
                    value: value.column(0).as_string::<i32>().value(0),
                    literal_type: value.column(1).as_string::<i32>().value(0),
                }),
            },
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'data UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::TypedLiteral => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let datatypes = array.column(1).as_string::<i32>();
                Ok(Self {
                    value: values.value(offset),
                    literal_type: datatypes.value(offset),
                })
            }
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for Boolean {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Boolean.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Boolean(Some(value)) => Ok(Self::from(*value)),
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Boolean => Ok(array
                .child(field.type_id())
                .as_boolean()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for Decimal {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Decimal.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Decimal128(Some(value), _, _) => {
                Ok(Decimal::from_be_bytes(value.to_be_bytes()))
            }
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Decimal => Ok(Decimal::from_be_bytes(
                array
                    .child(field.type_id())
                    .as_primitive::<Decimal128Type>()
                    .value(offset)
                    .to_be_bytes(),
            )),
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for Double {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Double.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Float64(Some(value)) => Ok((*value).into()),
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Double => Ok(array
                .child(field.type_id())
                .as_primitive::<Float64Type>()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for Float {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Float.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Float32(Some(value)) => Ok((*value).into()),
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Float => Ok(array
                .child(field.type_id())
                .as_primitive::<Float32Type>()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for Int {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Int.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok(Self::new(*value)),
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Int => Ok(array
                .child(field.type_id())
                .as_primitive::<Int32Type>()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for Integer {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Int.type_id() && *type_id != EncTermField::Integer.type_id() {
            return Err(());
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok((*value).into()),
            ScalarValue::Int64(Some(value)) => Ok((*value).into()),
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let offset = array.value_offset(index);

        match field {
            EncTermField::Int => Ok(array
                .child(field.type_id())
                .as_primitive::<Int32Type>()
                .value(offset)
                .into()),
            EncTermField::Integer => Ok(array
                .child(field.type_id())
                .as_primitive::<Int64Type>()
                .value(offset)
                .into()),
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for Numeric {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return Err(());
        };

        let field = EncTermField::try_from(*type_id).expect("Fixed encoding");
        match field {
            EncTermField::Int => Ok(Self::Int(Int::from_enc_scalar(scalar)?)),
            EncTermField::Integer => Ok(Self::Integer(Integer::from_enc_scalar(scalar)?)),
            EncTermField::Float => Ok(Self::Float(Float::from_enc_scalar(scalar)?)),
            EncTermField::Double => Ok(Self::Double(Double::from_enc_scalar(scalar)?)),
            EncTermField::Decimal => Ok(Self::Decimal(Decimal::from_enc_scalar(scalar)?)),
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        match field {
            EncTermField::Int => Ok(Self::Int(Int::from_sortable_array(array, index)?)),
            EncTermField::Integer => Ok(Self::Integer(Integer::from_sortable_array(array, index)?)),
            EncTermField::Float => Ok(Self::Float(Float::from_sortable_array(array, index)?)),
            EncTermField::Double => Ok(Self::Double(Double::from_sortable_array(array, index)?)),
            EncTermField::Decimal => Ok(Self::Decimal(Decimal::from_sortable_array(array, index)?)),
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for Duration {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (Some(year_month), Some(day_time)) => {
                Ok(Duration::new(year_month, day_time).map_err(|_| ())?)
            }
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (Some(year_month), Some(day_time)) => {
                Ok(Duration::new(year_month, day_time).map_err(|_| ())?)
            }
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for YearMonthDuration {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (Some(year_month), None) => {
                Ok(YearMonthDuration::new(year_month))
            }
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (Some(year_month), None) => {
                Ok(YearMonthDuration::new(year_month))
            }
            _ => Err(()),
        }
    }
}

impl FromSortableTerm<'_> for DayTimeDuration {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        match get_duration_encoding_scalar(scalar)? {
            (None, Some(day_time)) => {
                Ok(DayTimeDuration::new(day_time))
            }
            _ => Err(()),
        }
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        match get_duration_encoding_array(array, index)? {
            (None, Some(day_time)) => {
                Ok(DayTimeDuration::new(day_time))
            }
            _ => Err(()),
        }
    }
}


impl FromSortableTerm<'_> for DateTime {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::DateTime.type_id() {
            return Err(());
        }

        let timestamp = Timestamp::from_enc_scalar(scalar)?;
        Ok(DateTime::new(timestamp))
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        if field != EncTermField::DateTime {
            return Err(());
        }

        let timestamp = Timestamp::from_sortable_array(array, index)?;
        Ok(DateTime::new(timestamp))
    }
}


impl FromSortableTerm<'_> for Time {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Time.type_id() {
            return Err(());
        }

        let timestamp = Timestamp::from_enc_scalar(scalar)?;
        Ok(Time::new(timestamp))
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        if field != EncTermField::Time {
            return Err(());
        }

        let timestamp = Timestamp::from_sortable_array(array, index)?;
        Ok(Time::new(timestamp))
    }
}

impl FromSortableTerm<'_> for Date {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return Err(());
        };

        if *type_id != EncTermField::Date.type_id() {
            return Err(());
        }

        let timestamp = Timestamp::from_enc_scalar(scalar)?;
        Ok(Date::new(timestamp))
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        if field != EncTermField::Date {
            return Err(());
        }

        let timestamp = Timestamp::from_sortable_array(array, index)?;
        Ok(Date::new(timestamp))
    }
}

impl FromSortableTerm<'_> for Timestamp {
    fn from_enc_scalar(scalar: &'_ ScalarValue) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((_, value)), _, _) = scalar else {
            return Err(());
        };

        if value.data_type() != DataType::Struct(EncTerm::timestamp_fields()) {
            return Err(());
        }

        let ScalarValue::Struct(struct_array) = value.as_ref() else {
            unreachable!("Type already checked.")
        };

        let value = struct_array.column(0).as_primitive::<Decimal128Type>();
        let offset = struct_array.column(1).as_primitive::<Int16Type>();

        Ok(Timestamp::new(
            Decimal::from_be_bytes(value.value(0).to_be_bytes()),
            offset.is_null(0).not().then(|| TimezoneOffset::new_unchecked(offset.value(0)))
        ))
    }

    fn from_sortable_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<Self> {
        let offset = array.value_offset(index);
        let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
        let struct_array = array.child(field.type_id()).as_struct_opt().ok_or(())?;

        let value_array = struct_array.column(0).as_primitive::<Decimal128Type>();
        let offset_array = struct_array.column(1).as_primitive::<Int16Type>();

        Ok(Timestamp::new(
            Decimal::from_be_bytes(value_array.value(offset).to_be_bytes()),
            offset_array.is_null(offset).not().then(|| TimezoneOffset::new_unchecked(offset_array.value(offset)))
        ))
    }
}

fn get_duration_encoding_scalar(scalar: &ScalarValue) -> RdfOpResult<(Option<i64>, Option<Decimal>)> {
    let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
        return Err(());
    };

    let field = EncTermField::try_from(*type_id).expect("Fixed encoding");
    if field != EncTermField::Duration {
        return Err(());
    }

    let ScalarValue::Struct(struct_array) = scalar.as_ref() else {
        return Err(());
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

fn get_duration_encoding_array(array: &'_ UnionArray, index: usize) -> RdfOpResult<(Option<i64>, Option<Decimal>)> {
    let field = EncTermField::try_from(array.type_id(index)).expect("Fixed encoding");
    if field != EncTermField::Duration {
        return Err(());
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