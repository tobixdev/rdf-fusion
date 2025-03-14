use crate::datatypes::{RdfTerm, RdfValue, XsdBoolean, XsdDouble, XsdInt, XsdInteger, XsdNumeric};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{AsArray, UnionArray};
use datafusion::arrow::datatypes::Float32Type;
use datafusion::common::{internal_err, ScalarValue};
use std::cmp::Ordering;
use std::fmt;
use std::num::ParseFloatError;
use std::ops::{Add, Div, Mul, Neg, Sub};
use std::str::FromStr;

/// [XML Schema `float` datatype](https://www.w3.org/TR/xmlschema11-2/#float)
///
/// Uses internally a [`f32`].
///
/// <div class="warning">Serialization does not follow the canonical mapping.</div>
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[repr(transparent)]
pub struct XsdFloat {
    value: f32,
}

impl XsdFloat {
    pub const INFINITY: Self = Self {
        value: f32::INFINITY,
    };
    pub const MAX: Self = Self { value: f32::MAX };
    pub const MIN: Self = Self { value: f32::MIN };
    pub const NAN: Self = Self { value: f32::NAN };
    pub const NEG_INFINITY: Self = Self {
        value: f32::NEG_INFINITY,
    };

    pub fn as_f32(self) -> f32 {
        self.value
    }

    /// [fn:abs](https://www.w3.org/TR/xpath-functions-31/#func-abs)
    #[inline]
    #[must_use]
    pub fn abs(self) -> Self {
        self.value.abs().into()
    }

    /// [fn:ceiling](https://www.w3.org/TR/xpath-functions-31/#func-ceiling)
    #[inline]
    #[must_use]
    pub fn ceil(self) -> Self {
        self.value.ceil().into()
    }

    /// [fn:floor](https://www.w3.org/TR/xpath-functions-31/#func-floor)
    #[inline]
    #[must_use]
    pub fn floor(self) -> Self {
        self.value.floor().into()
    }

    /// [fn:round](https://www.w3.org/TR/xpath-functions-31/#func-round)
    #[inline]
    #[must_use]
    pub fn round(self) -> Self {
        self.value.round().into()
    }

    #[inline]
    #[must_use]
    pub fn is_nan(self) -> bool {
        self.value.is_nan()
    }

    #[inline]
    #[must_use]
    pub fn is_finite(self) -> bool {
        self.value.is_finite()
    }

    /// Checks if the two values are [identical](https://www.w3.org/TR/xmlschema11-2/#identity).
    #[inline]
    #[must_use]
    pub fn is_identical_with(self, other: Self) -> bool {
        self.value.to_bits() == other.value.to_bits()
    }
}

impl RdfValue<'_> for XsdFloat {
    fn from_term(term: RdfTerm<'_>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::Numeric(XsdNumeric::Float(inner)) => Ok(inner),
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

        if *type_id != EncTermField::Float.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Float32(Some(value)) => Ok(Self { value: *value }),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_enc_array(array: &UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            EncTermField::Float => Ok(Self {
                value: array
                    .child(field.type_id())
                    .as_primitive::<Float32Type>()
                    .value(offset),
            }),
            _ => internal_err!("Cannot create EncBoolean from {}.", field),
        }
    }
}

impl From<XsdFloat> for f32 {
    #[inline]
    fn from(value: XsdFloat) -> Self {
        value.value
    }
}

impl From<XsdFloat> for f64 {
    #[inline]
    fn from(value: XsdFloat) -> Self {
        value.value.into()
    }
}

impl From<f32> for XsdFloat {
    #[inline]
    fn from(value: f32) -> Self {
        Self { value }
    }
}

impl From<i8> for XsdFloat {
    #[inline]
    fn from(value: i8) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i16> for XsdFloat {
    #[inline]
    fn from(value: i16) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<u8> for XsdFloat {
    #[inline]
    fn from(value: u8) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<u16> for XsdFloat {
    #[inline]
    fn from(value: u16) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<XsdBoolean> for XsdFloat {
    #[inline]
    fn from(value: XsdBoolean) -> Self {
        f32::from(bool::from(value)).into()
    }
}

impl From<XsdInt> for XsdFloat {
    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn from(value: XsdInt) -> Self {
        (i32::from(value) as f32).into()
    }
}

impl From<XsdInteger> for XsdFloat {
    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn from(value: XsdInteger) -> Self {
        (i64::from(value) as f32).into()
    }
}

impl From<XsdDouble> for XsdFloat {
    #[inline]
    #[allow(clippy::cast_possible_truncation)]
    fn from(value: XsdDouble) -> Self {
        Self {
            value: f64::from(value) as f32,
        }
    }
}

impl FromStr for XsdFloat {
    type Err = ParseFloatError;

    #[inline]
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(f32::from_str(input)?.into())
    }
}

impl fmt::Display for XsdFloat {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.value == f32::INFINITY {
            f.write_str("INF")
        } else if self.value == f32::NEG_INFINITY {
            f.write_str("-INF")
        } else {
            self.value.fmt(f)
        }
    }
}

impl PartialOrd for XsdFloat {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl Neg for XsdFloat {
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        (-self.value).into()
    }
}

impl Add for XsdFloat {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self {
        (self.value + rhs.value).into()
    }
}

impl Sub for XsdFloat {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self {
        (self.value - rhs.value).into()
    }
}

impl Mul for XsdFloat {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Self) -> Self {
        (self.value * rhs.value).into()
    }
}

impl Div for XsdFloat {
    type Output = Self;

    #[inline]
    fn div(self, rhs: Self) -> Self {
        (self.value / rhs.value).into()
    }
}

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;

    #[test]
    fn eq() {
        assert_eq!(XsdFloat::from(0.), XsdFloat::from(0.));
        assert_ne!(XsdFloat::NAN, XsdFloat::NAN);
        assert_eq!(XsdFloat::from(-0.), XsdFloat::from(0.));
    }

    #[test]
    fn cmp() {
        assert_eq!(
            XsdFloat::from(0.).partial_cmp(&XsdFloat::from(0.)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            XsdFloat::INFINITY.partial_cmp(&XsdFloat::MAX),
            Some(Ordering::Greater)
        );
        assert_eq!(
            XsdFloat::NEG_INFINITY.partial_cmp(&XsdFloat::MIN),
            Some(Ordering::Less)
        );
        assert_eq!(XsdFloat::NAN.partial_cmp(&XsdFloat::from(0.)), None);
        assert_eq!(XsdFloat::NAN.partial_cmp(&XsdFloat::NAN), None);
        assert_eq!(
            XsdFloat::from(0.).partial_cmp(&XsdFloat::from(-0.)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn is_identical_with() {
        assert!(XsdFloat::from(0.).is_identical_with(XsdFloat::from(0.)));
        assert!(XsdFloat::NAN.is_identical_with(XsdFloat::NAN));
        assert!(!XsdFloat::from(-0.).is_identical_with(XsdFloat::from(0.)));
    }

    #[test]
    fn from_str() -> Result<(), ParseFloatError> {
        assert_eq!(XsdFloat::from_str("NaN")?.to_string(), "NaN");
        assert_eq!(XsdFloat::from_str("INF")?.to_string(), "INF");
        assert_eq!(XsdFloat::from_str("+INF")?.to_string(), "INF");
        assert_eq!(XsdFloat::from_str("-INF")?.to_string(), "-INF");
        assert_eq!(XsdFloat::from_str("0.0E0")?.to_string(), "0");
        assert_eq!(XsdFloat::from_str("-0.0E0")?.to_string(), "-0");
        assert_eq!(XsdFloat::from_str("0.1e1")?.to_string(), "1");
        assert_eq!(XsdFloat::from_str("-0.1e1")?.to_string(), "-1");
        assert_eq!(XsdFloat::from_str("1.e1")?.to_string(), "10");
        assert_eq!(XsdFloat::from_str("-1.e1")?.to_string(), "-10");
        assert_eq!(XsdFloat::from_str("1")?.to_string(), "1");
        assert_eq!(XsdFloat::from_str("-1")?.to_string(), "-1");
        assert_eq!(XsdFloat::from_str("1.")?.to_string(), "1");
        assert_eq!(XsdFloat::from_str("-1.")?.to_string(), "-1");
        assert_eq!(XsdFloat::from_str(&f32::MIN.to_string())?, XsdFloat::MIN);
        assert_eq!(XsdFloat::from_str(&f32::MAX.to_string())?, XsdFloat::MAX);
        Ok(())
    }
}
