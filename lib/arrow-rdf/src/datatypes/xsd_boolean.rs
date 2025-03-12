use crate::datatypes::rdf_term::RdfTerm;
use crate::datatypes::{RdfValue, XsdDecimal, XsdDouble, XsdFloat, XsdInteger};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{AsArray, UnionArray};
use datafusion::common::{internal_err, ScalarValue};
use std::fmt;
use std::str::{FromStr, ParseBoolError};

/// [XML Schema `boolean` datatype](https://www.w3.org/TR/xmlschema11-2/#boolean)
///
/// Uses internally a [`bool`].
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct XsdBoolean {
    value: bool,
}

impl XsdBoolean {
    pub fn as_bool(self) -> bool {
        self.value
    }

    /// Checks if the two values are [identical](https://www.w3.org/TR/xmlschema11-2/#identity).
    #[inline]
    #[must_use]
    pub fn is_identical_with(self, other: Self) -> bool {
        self == other
    }
}

impl RdfValue<'_> for XsdBoolean {
    fn from_term(term: RdfTerm<'_>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::Boolean(inner) => Ok(inner),
            _ => internal_err!("Unexpected EncRdfTerm"),
        }
    }

    fn from_enc_scalar(scalar: &'_ ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        if *type_id != EncTermField::Boolean.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Boolean(Some(value)) => Ok(Self::from(*value)),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_enc_array(array: &UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            EncTermField::Boolean => Ok(Self {
                value: array.child(field.type_id()).as_boolean().value(offset),
            }),
            _ => internal_err!("Cannot create EncBoolean from {}.", field),
        }
    }
}

impl From<bool> for XsdBoolean {
    #[inline]
    fn from(value: bool) -> Self {
        Self { value }
    }
}

impl From<XsdInteger> for XsdBoolean {
    #[inline]
    fn from(value: XsdInteger) -> Self {
        (value != XsdInteger::from(0)).into()
    }
}

impl From<XsdDecimal> for XsdBoolean {
    #[inline]
    fn from(value: XsdDecimal) -> Self {
        (value != XsdDecimal::from(0)).into()
    }
}

impl From<XsdFloat> for XsdBoolean {
    #[inline]
    fn from(value: XsdFloat) -> Self {
        (value != XsdFloat::from(0.) && !value.is_nan()).into()
    }
}

impl From<XsdDouble> for XsdBoolean {
    #[inline]
    fn from(value: XsdDouble) -> Self {
        (value != XsdDouble::from(0.) && !value.is_nan()).into()
    }
}

impl From<XsdBoolean> for bool {
    #[inline]
    fn from(value: XsdBoolean) -> Self {
        value.value
    }
}

impl FromStr for XsdBoolean {
    type Err = ParseBoolError;

    #[inline]
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(match input {
            "true" | "1" => true,
            "false" | "0" => false,
            _ => bool::from_str(input)?,
        }
        .into())
    }
}

impl fmt::Display for XsdBoolean {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;

    #[test]
    fn from_str() -> Result<(), ParseBoolError> {
        assert_eq!(XsdBoolean::from_str("true")?.to_string(), "true");
        assert_eq!(XsdBoolean::from_str("1")?.to_string(), "true");
        assert_eq!(XsdBoolean::from_str("false")?.to_string(), "false");
        assert_eq!(XsdBoolean::from_str("0")?.to_string(), "false");
        Ok(())
    }

    #[test]
    fn from_integer() {
        assert_eq!(XsdBoolean::from(false), XsdInteger::from(0).into());
        assert_eq!(XsdBoolean::from(true), XsdInteger::from(1).into());
        assert_eq!(XsdBoolean::from(true), XsdInteger::from(2).into());
    }

    #[test]
    fn from_decimal() {
        assert_eq!(XsdBoolean::from(false), XsdDecimal::from(0).into());
        assert_eq!(XsdBoolean::from(true), XsdDecimal::from(1).into());
        assert_eq!(XsdBoolean::from(true), XsdDecimal::from(2).into());
    }

    #[test]
    fn from_float() {
        assert_eq!(XsdBoolean::from(false), XsdFloat::from(0.).into());
        assert_eq!(XsdBoolean::from(true), XsdFloat::from(1.).into());
        assert_eq!(XsdBoolean::from(true), XsdFloat::from(2.).into());
        assert_eq!(XsdBoolean::from(false), XsdFloat::from(f32::NAN).into());
        assert_eq!(XsdBoolean::from(true), XsdFloat::from(f32::INFINITY).into());
    }

    #[test]
    fn from_double() {
        assert_eq!(XsdBoolean::from(false), XsdDouble::from(0.).into());
        assert_eq!(XsdBoolean::from(true), XsdDouble::from(1.).into());
        assert_eq!(XsdBoolean::from(true), XsdDouble::from(2.).into());
        assert_eq!(XsdBoolean::from(false), XsdDouble::from(f64::NAN).into());
        assert_eq!(
            XsdBoolean::from(true),
            XsdDouble::from(f64::INFINITY).into()
        );
    }
}
