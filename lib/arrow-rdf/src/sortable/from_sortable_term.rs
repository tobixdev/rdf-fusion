use crate::encoded::EncTermField;
use crate::sortable::term_type::SortableTermType;
use crate::sortable::SortableTermField;
use datafusion::arrow::array::{Array, AsArray, StructArray};
use datafusion::arrow::datatypes::UInt8Type;
use model::{BlankNodeRef, NamedNodeRef};
use model::{
    Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int, Integer,
    InternalTermRef, LanguageStringRef, Numeric, SimpleLiteralRef, ThinError, ThinResult, Time,
    TypedLiteralRef, YearMonthDuration,
};

pub trait FromSortableTerm<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> ThinResult<Self>
    where
        Self: Sized;
}

impl<'data> FromSortableTerm<'data> for InternalTermRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> ThinResult<Self>
    where
        Self: Sized,
    {
        let enc_term_type = array
            .column(SortableTermField::EncTermType.index())
            .as_primitive::<UInt8Type>()
            .value(index);
        let enc_term_field = EncTermField::try_from(enc_term_type)?;

        let sortable_type = array
            .column(SortableTermField::Type.index())
            .as_primitive::<UInt8Type>();
        let sortable_term_type = SortableTermType::try_from(sortable_type.value(index))?;

        let result = match enc_term_field {
            EncTermField::Null => return ThinError::expected(),
            EncTermField::NamedNode => {
                InternalTermRef::NamedNode(NamedNodeRef::from_sortable_array(array, index)?)
            }
            EncTermField::BlankNode => {
                InternalTermRef::BlankNode(BlankNodeRef::from_sortable_array(array, index)?)
            }
            EncTermField::String => {
                if array
                    .column(SortableTermField::AdditionalBytes.index())
                    .is_null(index)
                {
                    InternalTermRef::SimpleLiteral(SimpleLiteralRef::from_sortable_array(
                        array, index,
                    )?)
                } else {
                    InternalTermRef::LanguageStringLiteral(LanguageStringRef::from_sortable_array(
                        array, index,
                    )?)
                }
            }
            EncTermField::Boolean => {
                InternalTermRef::BooleanLiteral(Boolean::from_sortable_array(array, index)?)
            }
            EncTermField::Float => InternalTermRef::NumericLiteral(Numeric::Float(
                Float::from_sortable_array(array, index)?,
            )),
            EncTermField::Double => InternalTermRef::NumericLiteral(Numeric::Double(
                Double::from_sortable_array(array, index)?,
            )),
            EncTermField::Decimal => InternalTermRef::NumericLiteral(Numeric::Decimal(
                Decimal::from_sortable_array(array, index)?,
            )),
            EncTermField::Int => InternalTermRef::NumericLiteral(Numeric::Int(
                Int::from_sortable_array(array, index)?,
            )),
            EncTermField::Integer => InternalTermRef::NumericLiteral(Numeric::Integer(
                Integer::from_sortable_array(array, index)?,
            )),
            EncTermField::DateTime => {
                InternalTermRef::DateTimeLiteral(DateTime::from_sortable_array(array, index)?)
            }
            EncTermField::Time => {
                InternalTermRef::TimeLiteral(Time::from_sortable_array(array, index)?)
            }
            EncTermField::Date => {
                InternalTermRef::DateLiteral(Date::from_sortable_array(array, index)?)
            }
            EncTermField::Duration => match sortable_term_type {
                SortableTermType::Duration => {
                    InternalTermRef::DurationLiteral(Duration::from_sortable_array(array, index)?)
                }
                SortableTermType::YearMonthDuration => InternalTermRef::YearMonthDurationLiteral(
                    YearMonthDuration::from_sortable_array(array, index)?,
                ),
                SortableTermType::DayTimeDuration => InternalTermRef::DayTimeDurationLiteral(
                    DayTimeDuration::from_sortable_array(array, index)?,
                ),
                _ => unreachable!("Cannot not have EncTermField::Duration"),
            },
            EncTermField::TypedLiteral => {
                InternalTermRef::TypedLiteral(TypedLiteralRef::from_sortable_array(array, index)?)
            }
        };
        Ok(result)
    }
}

impl<'data> FromSortableTerm<'data> for BlankNodeRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::BlankNode)?;
        let string = std::str::from_utf8(bytes)
            .map_err(|_| ThinError::InternalError("Invalid UTF8 'Bytes' for BlankNodeRef."))?;
        Ok(BlankNodeRef::new_unchecked(string))
    }
}

impl<'data> FromSortableTerm<'data> for NamedNodeRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::NamedNode)?;
        let string = std::str::from_utf8(bytes)
            .map_err(|_| ThinError::InternalError("Invalid UTF8 'Bytes' for NamedNodeRef."))?;
        Ok(NamedNodeRef::new_unchecked(string))
    }
}

impl<'data> FromSortableTerm<'data> for SimpleLiteralRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> ThinResult<Self> {
        if !array
            .column(SortableTermField::AdditionalBytes.index())
            .is_null(index)
        {
            return ThinError::expected();
        }

        let bytes = try_obtain_bytes(array, index, EncTermField::String)?;
        let string = std::str::from_utf8(bytes)
            .map_err(|_| ThinError::InternalError("Invalid UTF8 'Bytes' for SimpleLiteralRef."))?;
        Ok(SimpleLiteralRef::new(string))
    }
}

impl<'data> FromSortableTerm<'data> for LanguageStringRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> ThinResult<Self> {
        if array
            .column(SortableTermField::AdditionalBytes.index())
            .is_null(index)
        {
            return ThinError::expected();
        }

        let bytes = try_obtain_bytes(array, index, EncTermField::String)?;
        let string = std::str::from_utf8(bytes)
            .map_err(|_| ThinError::InternalError("Invalid UTF8 'Bytes' for LanguageStringRef."))?;

        let additional_bytes = try_obtain_bytes_from_field(
            array,
            SortableTermField::AdditionalBytes,
            index,
            EncTermField::String,
        )?;
        let additional_string = std::str::from_utf8(additional_bytes).map_err(|_| {
            ThinError::InternalError("Invalid UTF8 'AdditionalBytes' for LanguageStringRef.")
        })?;

        Ok(LanguageStringRef {
            value: string,
            language: additional_string,
        })
    }
}

impl<'data> FromSortableTerm<'data> for TypedLiteralRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> ThinResult<Self> {
        if array
            .column(SortableTermField::AdditionalBytes.index())
            .is_null(index)
        {
            return ThinError::expected();
        }

        let bytes = try_obtain_bytes(array, index, EncTermField::TypedLiteral)?;
        let string = std::str::from_utf8(bytes)
            .map_err(|_| ThinError::InternalError("Invalid UTF8 'Bytes' for TypedLiteralRef."))?;

        let additional_bytes = try_obtain_bytes_from_field(
            array,
            SortableTermField::AdditionalBytes,
            index,
            EncTermField::TypedLiteral,
        )?;
        let additional_string = std::str::from_utf8(additional_bytes).map_err(|_| {
            ThinError::InternalError("Invalid UTF8 'AdditionalBytes' for TypedLiteralRef.")
        })?;

        Ok(TypedLiteralRef {
            value: string,
            literal_type: additional_string,
        })
    }
}

const BYTE_LENGTH_ERROR: ThinError =
    ThinError::InternalError("The byte slice length did not match the expected one.");

impl FromSortableTerm<'_> for Boolean {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Boolean)?;
        Ok(Boolean::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for Decimal {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Decimal)?;
        Ok(Decimal::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for Double {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Double)?;
        Ok(Double::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for Float {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Float)?;
        Ok(Float::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for Int {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Int)?;
        Ok(Int::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for Integer {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Integer)?;
        Ok(Integer::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for Duration {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Duration)?;
        Ok(Duration::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for YearMonthDuration {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Duration)?;
        Ok(Duration::from_be_bytes(bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?).year_month())
    }
}

impl FromSortableTerm<'_> for DayTimeDuration {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Duration)?;
        Ok(Duration::from_be_bytes(bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?).day_time())
    }
}

impl FromSortableTerm<'_> for DateTime {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::DateTime)?;
        Ok(DateTime::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for Time {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Time)?;
        Ok(Time::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

impl FromSortableTerm<'_> for Date {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> ThinResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Date)?;
        Ok(Date::from_be_bytes(
            bytes.try_into().map_err(|_| BYTE_LENGTH_ERROR)?,
        ))
    }
}

fn try_obtain_bytes(
    array: &StructArray,
    index: usize,
    expected_field: EncTermField,
) -> ThinResult<&[u8]> {
    try_obtain_bytes_from_field(array, SortableTermField::Bytes, index, expected_field)
}

fn try_obtain_bytes_from_field(
    array: &StructArray,
    field: SortableTermField,
    index: usize,
    expected_field: EncTermField,
) -> ThinResult<&[u8]> {
    let enc_term_type = array
        .column(SortableTermField::EncTermType.index())
        .as_primitive::<UInt8Type>()
        .value(index);
    let enc_term_type = i8::try_from(enc_term_type)
        .map_err(|_| ThinError::InternalError("Could not convert EncTermType to i8."))?;
    let enc_term_type = EncTermField::try_from(enc_term_type)?;

    if enc_term_type != expected_field {
        return ThinError::expected();
    }

    Ok(array.column(field.index()).as_binary::<i32>().value(index))
}
