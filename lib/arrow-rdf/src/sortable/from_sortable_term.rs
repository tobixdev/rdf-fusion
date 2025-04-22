use crate::encoded::EncTermField;
use crate::sortable::term_type::SortableTermType;
use crate::sortable::SortableTermField;
use datafusion::arrow::array::{Array, AsArray, StructArray};
use datafusion::arrow::datatypes::UInt8Type;
use datamodel::{
    Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int, Integer,
    LanguageStringRef, Numeric, RdfOpResult, SimpleLiteralRef, TermRef, Time, TypedLiteralRef,
    YearMonthDuration,
};
use oxrdf::{BlankNodeRef, NamedNodeRef};

pub trait FromSortableTerm<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> RdfOpResult<Self>
    where
        Self: Sized;
}

impl<'data> FromSortableTerm<'data> for TermRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> RdfOpResult<Self>
    where
        Self: Sized,
    {
        let enc_term_type = array
            .column(SortableTermField::EncTermType.index())
            .as_primitive::<UInt8Type>();
        let enc_term_field = EncTermField::try_from(enc_term_type.value(index) as i8)
            .expect("Conversion should always succeed.");

        let sortable_type = array
            .column(SortableTermField::Type.index())
            .as_primitive::<UInt8Type>();
        let sortable_term_type = SortableTermType::try_from(sortable_type.value(index))
            .expect("Conversion should always succeed.");

        let result = match enc_term_field {
            EncTermField::Null => return Err(()),
            EncTermField::NamedNode => {
                TermRef::NamedNode(NamedNodeRef::from_sortable_array(array, index)?)
            }
            EncTermField::BlankNode => {
                TermRef::BlankNode(BlankNodeRef::from_sortable_array(array, index)?)
            }
            EncTermField::String => {
                if array
                    .column(SortableTermField::AdditionalBytes.index())
                    .is_null(index)
                {
                    TermRef::SimpleLiteral(SimpleLiteralRef::from_sortable_array(array, index)?)
                } else {
                    TermRef::LanguageStringLiteral(LanguageStringRef::from_sortable_array(
                        array, index,
                    )?)
                }
            }
            EncTermField::Boolean => {
                TermRef::BooleanLiteral(Boolean::from_sortable_array(array, index)?)
            }
            EncTermField::Float => {
                TermRef::NumericLiteral(Numeric::Float(Float::from_sortable_array(array, index)?))
            }
            EncTermField::Double => {
                TermRef::NumericLiteral(Numeric::Double(Double::from_sortable_array(array, index)?))
            }
            EncTermField::Decimal => TermRef::NumericLiteral(Numeric::Decimal(
                Decimal::from_sortable_array(array, index)?,
            )),
            EncTermField::Int => {
                TermRef::NumericLiteral(Numeric::Int(Int::from_sortable_array(array, index)?))
            }
            EncTermField::Integer => TermRef::NumericLiteral(Numeric::Integer(
                Integer::from_sortable_array(array, index)?,
            )),
            EncTermField::DateTime => {
                TermRef::DateTimeLiteral(DateTime::from_sortable_array(array, index)?)
            }
            EncTermField::Time => TermRef::TimeLiteral(Time::from_sortable_array(array, index)?),
            EncTermField::Date => TermRef::DateLiteral(Date::from_sortable_array(array, index)?),
            EncTermField::Duration => match sortable_term_type {
                SortableTermType::Duration => {
                    TermRef::DurationLiteral(Duration::from_sortable_array(array, index)?)
                }
                SortableTermType::YearMonthDuration => TermRef::YearMonthDurationLiteral(
                    YearMonthDuration::from_sortable_array(array, index)?,
                ),
                SortableTermType::DayTimeDuration => TermRef::DayTimeDurationLiteral(
                    DayTimeDuration::from_sortable_array(array, index)?,
                ),
                _ => unreachable!("Cannot not have EncTermField::Duration"),
            },
            EncTermField::TypedLiteral => {
                TermRef::TypedLiteral(TypedLiteralRef::from_sortable_array(array, index)?)
            }
        };
        Ok(result)
    }
}

impl<'data> FromSortableTerm<'data> for BlankNodeRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::BlankNode)?;
        let string = std::str::from_utf8(bytes).expect("Term field checked");
        Ok(BlankNodeRef::new_unchecked(string))
    }
}

impl<'data> FromSortableTerm<'data> for NamedNodeRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::NamedNode)?;
        let string = std::str::from_utf8(bytes).expect("Term field checked");
        Ok(NamedNodeRef::new_unchecked(string))
    }
}

impl<'data> FromSortableTerm<'data> for SimpleLiteralRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> RdfOpResult<Self> {
        if !array
            .column(SortableTermField::AdditionalBytes.index())
            .is_null(index)
        {
            return Err(());
        }

        let bytes = try_obtain_bytes(array, index, EncTermField::String)?;
        let string = std::str::from_utf8(bytes).expect("Term field checked");
        Ok(SimpleLiteralRef::new(string))
    }
}

impl<'data> FromSortableTerm<'data> for LanguageStringRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> RdfOpResult<Self> {
        if array
            .column(SortableTermField::AdditionalBytes.index())
            .is_null(index)
        {
            return Err(());
        }

        let bytes = try_obtain_bytes(array, index, EncTermField::String)?;
        let string = std::str::from_utf8(bytes).expect("Term field checked");

        let additional_bytes = try_obtain_bytes_from_field(
            array,
            SortableTermField::AdditionalBytes,
            index,
            EncTermField::String,
        )?;
        let additional_string = std::str::from_utf8(additional_bytes).expect("Term field checked");

        Ok(LanguageStringRef {
            value: string,
            language: additional_string,
        })
    }
}

impl<'data> FromSortableTerm<'data> for TypedLiteralRef<'data> {
    fn from_sortable_array(array: &'data StructArray, index: usize) -> RdfOpResult<Self> {
        if array
            .column(SortableTermField::AdditionalBytes.index())
            .is_null(index)
        {
            return Err(());
        }

        let bytes = try_obtain_bytes(array, index, EncTermField::TypedLiteral)?;
        let string = std::str::from_utf8(bytes).expect("Term field checked");

        let additional_bytes = try_obtain_bytes_from_field(
            array,
            SortableTermField::AdditionalBytes,
            index,
            EncTermField::TypedLiteral,
        )?;
        let additional_string = std::str::from_utf8(additional_bytes).expect("Term field checked");

        Ok(TypedLiteralRef {
            value: string,
            literal_type: additional_string,
        })
    }
}

impl FromSortableTerm<'_> for Boolean {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Boolean)?;
        Ok(Boolean::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for Decimal {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Decimal)?;
        Ok(Decimal::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for Double {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Double)?;
        Ok(Double::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for Float {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Float)?;
        Ok(Float::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for Int {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Int)?;
        Ok(Int::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for Integer {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Integer)?;
        Ok(Integer::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for Duration {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Duration)?;
        Ok(Duration::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for YearMonthDuration {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Duration)?;
        Ok(Duration::from_be_bytes(bytes.try_into().map_err(|_| ())?).year_month())
    }
}

impl FromSortableTerm<'_> for DayTimeDuration {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Duration)?;
        Ok(Duration::from_be_bytes(bytes.try_into().map_err(|_| ())?).day_time())
    }
}

impl FromSortableTerm<'_> for DateTime {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::DateTime)?;
        Ok(DateTime::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for Time {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Time)?;
        Ok(Time::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

impl FromSortableTerm<'_> for Date {
    fn from_sortable_array(array: &'_ StructArray, index: usize) -> RdfOpResult<Self> {
        let bytes = try_obtain_bytes(array, index, EncTermField::Date)?;
        Ok(Date::from_be_bytes(bytes.try_into().map_err(|_| ())?))
    }
}

fn try_obtain_bytes(
    array: &StructArray,
    index: usize,
    expected_field: EncTermField,
) -> RdfOpResult<&[u8]> {
    try_obtain_bytes_from_field(array, SortableTermField::Bytes, index, expected_field)
}

fn try_obtain_bytes_from_field(
    array: &StructArray,
    field: SortableTermField,
    index: usize,
    expected_field: EncTermField,
) -> RdfOpResult<&[u8]> {
    let enc_term_type = array
        .column(SortableTermField::EncTermType.index())
        .as_primitive::<UInt8Type>()
        .value(index);
    let enc_term_type =
        EncTermField::try_from(enc_term_type as i8).expect("We only encode valid values.");

    if enc_term_type != expected_field {
        return Err(());
    }

    Ok(array.column(field.index()).as_binary::<i32>().value(index))
}
