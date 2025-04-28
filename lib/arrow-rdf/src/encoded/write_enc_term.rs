use crate::encoded::EncRdfTermBuilder;
use crate::DFResult;
use datafusion::arrow::array::ArrayRef;
use datafusion::common::{exec_err, ScalarValue};
use model::{BlankNode, BlankNodeRef, NamedNode, NamedNodeRef};
use model::{
    Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int, Integer,
    InternalTermRef, LanguageStringRef, Numeric, OwnedStringLiteral, SimpleLiteralRef,
    StringLiteralRef, ThinError, ThinResult, Time, TypedLiteralRef, YearMonthDuration,
};

pub trait WriteEncTerm {
    fn into_scalar_value(self) -> DFResult<ScalarValue>
    where
        Self: Sized,
    {
        let array = Self::iter_into_array([Ok(self)].into_iter())?;
        ScalarValue::try_from_array(&array, 0)
    }

    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized;
}

impl WriteEncTerm for Boolean {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_boolean(value.as_bool())?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Float {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_float(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Decimal {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_decimal(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Double {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_double(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Int {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_int(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Integer {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_integer(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Numeric {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(Numeric::Float(value)) => rdf_term_builder.append_float(value)?,
                Ok(Numeric::Double(value)) => rdf_term_builder.append_double(value)?,
                Ok(Numeric::Decimal(value)) => rdf_term_builder.append_decimal(value)?,
                Ok(Numeric::Int(value)) => rdf_term_builder.append_int(value)?,
                Ok(Numeric::Integer(value)) => rdf_term_builder.append_integer(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for SimpleLiteralRef<'_> {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_string(value.value, None)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for StringLiteralRef<'_> {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_string(value.0, value.1)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for OwnedStringLiteral {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => {
                    rdf_term_builder.append_string(value.0.as_str(), value.1.as_deref())?
                }
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for LanguageStringRef<'_> {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_string(value.value, Some(value.language))?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for BlankNodeRef<'_> {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_blank_node(value.as_str())?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for BlankNode {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_blank_node(value.as_str())?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for NamedNodeRef<'_> {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_named_node(value.as_str())?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for NamedNode {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_named_node(value.as_str())?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for InternalTermRef<'_> {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(InternalTermRef::NamedNode(value)) => {
                    rdf_term_builder.append_named_node(value.as_str())?
                }
                Ok(InternalTermRef::BlankNode(value)) => {
                    rdf_term_builder.append_blank_node(value.as_str())?
                }
                Ok(InternalTermRef::BooleanLiteral(value)) => {
                    rdf_term_builder.append_boolean(value.as_bool())?
                }
                Ok(InternalTermRef::NumericLiteral(Numeric::Float(value))) => {
                    rdf_term_builder.append_float(value)?
                }
                Ok(InternalTermRef::NumericLiteral(Numeric::Double(value))) => {
                    rdf_term_builder.append_double(value)?
                }
                Ok(InternalTermRef::NumericLiteral(Numeric::Decimal(value))) => {
                    rdf_term_builder.append_decimal(value)?
                }
                Ok(InternalTermRef::NumericLiteral(Numeric::Int(value))) => {
                    rdf_term_builder.append_int(value)?
                }
                Ok(InternalTermRef::NumericLiteral(Numeric::Integer(value))) => {
                    rdf_term_builder.append_integer(value)?
                }
                Ok(InternalTermRef::SimpleLiteral(value)) => {
                    rdf_term_builder.append_string(value.value, None)?
                }
                Ok(InternalTermRef::LanguageStringLiteral(value)) => {
                    rdf_term_builder.append_string(value.value, Some(value.language))?
                }
                Ok(InternalTermRef::DateTimeLiteral(value)) => {
                    rdf_term_builder.append_date_time(value)?
                }
                Ok(InternalTermRef::TimeLiteral(value)) => rdf_term_builder.append_time(value)?,
                Ok(InternalTermRef::DateLiteral(value)) => rdf_term_builder.append_date(value)?,
                Ok(InternalTermRef::DurationLiteral(value)) => rdf_term_builder
                    .append_duration(Some(value.year_month()), Some(value.day_time()))?,
                Ok(InternalTermRef::YearMonthDurationLiteral(value)) => {
                    rdf_term_builder.append_duration(Some(value), None)?
                }
                Ok(InternalTermRef::DayTimeDurationLiteral(value)) => {
                    rdf_term_builder.append_duration(None, Some(value))?
                }
                Ok(InternalTermRef::TypedLiteral(value)) => {
                    rdf_term_builder.append_typed_literal(value.value, value.literal_type)?
                }
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for DateTime {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_date_time(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Time {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_time(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Date {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_date(value)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for Duration {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder
                    .append_duration(Some(value.year_month()), Some(value.day_time()))?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for YearMonthDuration {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_duration(Some(value), None)?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for DayTimeDuration {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_duration(None, Some(value))?,
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}

impl WriteEncTerm for TypedLiteralRef<'_> {
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::default();
        for value in values {
            match value {
                Ok(value) => {
                    rdf_term_builder.append_typed_literal(value.value, value.literal_type)?
                }
                Err(ThinError::Expected) => rdf_term_builder.append_null()?,
                Err(ThinError::InternalError(cause)) => {
                    return exec_err!("Internal error during RDF operation: {cause}")
                }
            }
        }
        rdf_term_builder.finish()
    }
}
