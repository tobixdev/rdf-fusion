use crate::encoded::EncRdfTermBuilder;
use crate::AResult;
use datafusion::arrow::array::ArrayRef;
use datafusion::common::ScalarValue;
use datamodel::{
    Boolean, Decimal, Double, Duration, Float, Int, Integer, LanguageStringRef, Numeric,
    OwnedStringLiteral, RdfOpResult, SimpleLiteralRef, StringLiteralRef, TermRef, TypedLiteralRef,
};
use oxrdf::{BlankNodeRef, NamedNode, NamedNodeRef};

pub trait WriteEncTerm {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized;

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized;
}

impl WriteEncTerm for Boolean {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_boolean(self.as_bool())?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_boolean(value.as_bool())?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for Float {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_float(self)?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_float(value)?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for Decimal {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_decimal(self)?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_decimal(value)?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for Double {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_double(self)?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_double(value)?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for Int {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_int(self)?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_int(value)?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for Integer {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_integer(self)?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_integer(value)?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for Numeric {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        match self {
            Numeric::Int(value) => value.into_scalar_value(),
            Numeric::Integer(value) => value.into_scalar_value(),
            Numeric::Float(value) => value.into_scalar_value(),
            Numeric::Double(value) => value.into_scalar_value(),
            Numeric::Decimal(value) => value.into_scalar_value(),
        }
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(Numeric::Float(value)) => rdf_term_builder.append_float(value)?,
                Ok(Numeric::Double(value)) => rdf_term_builder.append_double(value)?,
                Ok(Numeric::Decimal(value)) => rdf_term_builder.append_decimal(value)?,
                Ok(Numeric::Int(value)) => rdf_term_builder.append_int(value)?,
                Ok(Numeric::Integer(value)) => rdf_term_builder.append_integer(value)?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for SimpleLiteralRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_string(self.value, None)?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_string(value.value, None)?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for StringLiteralRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_string(self.0, self.1)?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_string(value.0, value.1)?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for OwnedStringLiteral {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_string(self.0.as_str(), self.1.as_ref().map(|v| v.as_str()))?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder
                    .append_string(value.0.as_str(), value.1.as_ref().map(|v| v.as_str()))?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for LanguageStringRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_string(self.value, Some(self.language))?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for BlankNodeRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_blank_node(self.as_str())?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_blank_node(value.as_str())?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for NamedNodeRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_named_node(self.as_str())?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_named_node(value.as_str())?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for NamedNode {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_named_node(self.as_str())?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(value) => rdf_term_builder.append_named_node(value.as_str())?,
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for TermRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        match self {
            TermRef::NamedNode(value) => value.into_scalar_value(),
            TermRef::BlankNode(value) => value.into_scalar_value(),
            TermRef::Boolean(value) => value.into_scalar_value(),
            TermRef::Numeric(value) => value.into_scalar_value(),
            TermRef::SimpleLiteral(value) => value.into_scalar_value(),
            TermRef::LanguageString(value) => value.into_scalar_value(),
            TermRef::Duration(value) => value.into_scalar_value(),
            TermRef::TypedLiteral(value) => value.into_scalar_value(),
        }
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        for value in values {
            match value {
                Ok(TermRef::NamedNode(value)) => {
                    rdf_term_builder.append_named_node(value.as_str())?
                }
                Ok(TermRef::BlankNode(value)) => {
                    rdf_term_builder.append_blank_node(value.as_str())?
                }
                Ok(TermRef::Boolean(value)) => rdf_term_builder.append_boolean(value.as_bool())?,
                Ok(TermRef::Numeric(Numeric::Float(value))) => {
                    rdf_term_builder.append_float(value)?
                }
                Ok(TermRef::Numeric(Numeric::Double(value))) => {
                    rdf_term_builder.append_double(value)?
                }
                Ok(TermRef::Numeric(Numeric::Decimal(value))) => {
                    rdf_term_builder.append_decimal(value)?
                }
                Ok(TermRef::Numeric(Numeric::Int(value))) => rdf_term_builder.append_int(value)?,
                Ok(TermRef::Numeric(Numeric::Integer(value))) => {
                    rdf_term_builder.append_integer(value)?
                }
                Ok(TermRef::SimpleLiteral(value)) => {
                    rdf_term_builder.append_string(value.value, None)?
                }
                Ok(TermRef::LanguageString(value)) => {
                    rdf_term_builder.append_string(value.value, Some(value.language))?
                }
                Ok(TermRef::Duration(value)) => {
                    todo!()
                }
                Ok(TermRef::TypedLiteral(value)) => {
                    rdf_term_builder.append_typed_literal(value.value, value.literal_type)?
                }
                Err(_) => rdf_term_builder.append_null()?,
            }
        }
        Ok(rdf_term_builder.finish()?)
    }
}

impl WriteEncTerm for Duration {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        todo!()
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}

impl WriteEncTerm for TypedLiteralRef<'_> {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized,
    {
        let mut rdf_term_builder = EncRdfTermBuilder::new();
        rdf_term_builder.append_typed_literal(self.value, self.literal_type)?;
        let array = rdf_term_builder.finish()?;
        Ok(ScalarValue::try_from_array(&array, 0)?)
    }

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized,
    {
        todo!()
    }
}
