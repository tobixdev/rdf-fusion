use crate::value_encoding::{ValueArrayBuilder, TermValueEncoding};
use crate::DFResult;
use crate::ToArrow;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::exec_err;
use model::{BlankNode, BlankNodeRef, LiteralRef, NamedNode, NamedNodeRef};
use model::{
    Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int, Integer,
    LanguageStringRef, Numeric, OwnedStringLiteral, SimpleLiteralRef, StringLiteralRef,
    TermValueRef, ThinError, ThinResult, Time, YearMonthDuration,
};

impl ToArrow for Boolean {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_boolean(value.as_bool())?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Float {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_float(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Decimal {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_decimal(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Double {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_double(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Int {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_int(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Integer {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_integer(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Numeric {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(Numeric::Float(value)) => value_builder.append_float(value)?,
                Ok(Numeric::Double(value)) => value_builder.append_double(value)?,
                Ok(Numeric::Decimal(value)) => value_builder.append_decimal(value)?,
                Ok(Numeric::Int(value)) => value_builder.append_int(value)?,
                Ok(Numeric::Integer(value)) => value_builder.append_integer(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for SimpleLiteralRef<'_> {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_string(value.value, None)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for StringLiteralRef<'_> {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_string(value.0, value.1)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for OwnedStringLiteral {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_string(value.0.as_str(), value.1.as_deref())?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for LanguageStringRef<'_> {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_string(value.value, Some(value.language))?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for BlankNodeRef<'_> {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_blank_node(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for BlankNode {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_blank_node(value.as_ref())?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for NamedNodeRef<'_> {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_named_node(value.as_str())?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for NamedNode {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_named_node(value.as_str())?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for DateTime {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_date_time(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Time {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_time(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Date {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_date(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for Duration {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder
                    .append_duration(Some(value.year_month()), Some(value.day_time()))?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for YearMonthDuration {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_duration(Some(value), None)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for DayTimeDuration {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_duration(None, Some(value))?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for LiteralRef<'_> {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(value) => value_builder.append_typed_literal(value)?,
                Err(ThinError::Expected) => value_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        Ok(value_builder.finish())
    }
}

impl ToArrow for TermValueRef<'_> {
    fn encoded_datatype() -> DataType {
        TermValueEncoding::datatype()
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut value_builder = ValueArrayBuilder::default();
        for value in values {
            match value {
                Ok(TermValueRef::NamedNode(value)) => {
                    value_builder.append_named_node(value.as_str())?
                }
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
        Ok(value_builder.finish())
    }
}
