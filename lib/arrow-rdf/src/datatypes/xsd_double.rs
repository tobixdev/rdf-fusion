use crate::datatypes::{RdfTerm, RdfValue, XsdBoolean, XsdFloat, XsdInt, XsdInteger, XsdNumeric};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{AsArray, UnionArray};
use datafusion::arrow::datatypes::Float64Type;
use datafusion::common::{internal_err, ScalarValue};
use std::cmp::Ordering;
use std::fmt;
use std::num::ParseFloatError;
use std::ops::{Add, Div, Mul, Neg, Sub};
use std::str::FromStr;

/// [XML Schema `double` datatype](https://www.w3.org/TR/xmlschema11-2/#double)
///
/// Uses internally a [`f64`].
///
/// <div class="warning">Serialization does not follow the canonical mapping.</div>
#[derive(Debug, Clone, Copy, Default, PartialEq)]
#[repr(transparent)]
pub struct XsdDouble {
    value: f64,
}

impl XsdDouble {
    pub const INFINITY: Self = Self {
        value: f64::INFINITY,
    };
    pub const MAX: Self = Self { value: f64::MAX };
    pub const MIN: Self = Self { value: f64::MIN };
    pub const NAN: Self = Self { value: f64::NAN };
    pub const NEG_INFINITY: Self = Self {
        value: f64::NEG_INFINITY,
    };

    pub fn as_f64(self) -> f64 {
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

impl RdfValue<'_> for XsdDouble {
    fn from_term(term: RdfTerm<'_>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::Numeric(XsdNumeric::Double(inner)) => Ok(inner),
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

        if *type_id != EncTermField::Double.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Float64(Some(value)) => Ok(Self { value: *value }),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_enc_array(array: &UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            EncTermField::Double => Ok(Self {
                value: array
                    .child(field.type_id())
                    .as_primitive::<Float64Type>()
                    .value(offset),
            }),
            _ => internal_err!("Cannot create EncBoolean from {}.", field),
        }
    }
}

impl From<XsdDouble> for f64 {
    #[inline]
    fn from(value: XsdDouble) -> Self {
        value.value
    }
}

impl From<f64> for XsdDouble {
    #[inline]
    fn from(value: f64) -> Self {
        Self { value }
    }
}

impl From<i8> for XsdDouble {
    #[inline]
    fn from(value: i8) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i16> for XsdDouble {
    #[inline]
    fn from(value: i16) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i32> for XsdDouble {
    #[inline]
    fn from(value: i32) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<u8> for XsdDouble {
    #[inline]
    fn from(value: u8) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<u16> for XsdDouble {
    #[inline]
    fn from(value: u16) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<u32> for XsdDouble {
    #[inline]
    fn from(value: u32) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<XsdFloat> for XsdDouble {
    #[inline]
    fn from(value: XsdFloat) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<XsdBoolean> for XsdDouble {
    #[inline]
    fn from(value: XsdBoolean) -> Self {
        f64::from(bool::from(value)).into()
    }
}

impl From<XsdInt> for XsdDouble {
    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn from(value: XsdInt) -> Self {
        (i32::from(value) as f64).into()
    }
}

impl From<XsdInteger> for XsdDouble {
    #[inline]
    #[allow(clippy::cast_precision_loss)]
    fn from(value: XsdInteger) -> Self {
        (i64::from(value) as f64).into()
    }
}

impl FromStr for XsdDouble {
    type Err = ParseFloatError;

    #[inline]
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(f64::from_str(input)?.into())
    }
}

impl fmt::Display for XsdDouble {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.value == f64::INFINITY {
            f.write_str("INF")
        } else if self.value == f64::NEG_INFINITY {
            f.write_str("-INF")
        } else {
            self.value.fmt(f)
        }
    }
}

impl PartialOrd for XsdDouble {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.value.partial_cmp(&other.value)
    }
}

impl Neg for XsdDouble {
    type Output = Self;

    #[inline]
    fn neg(self) -> Self {
        (-self.value).into()
    }
}

impl Add for XsdDouble {
    type Output = Self;

    #[inline]
    fn add(self, rhs: Self) -> Self {
        (self.value + rhs.value).into()
    }
}

impl Sub for XsdDouble {
    type Output = Self;

    #[inline]
    fn sub(self, rhs: Self) -> Self {
        (self.value - rhs.value).into()
    }
}

impl Mul for XsdDouble {
    type Output = Self;

    #[inline]
    fn mul(self, rhs: Self) -> Self {
        (self.value * rhs.value).into()
    }
}

impl Div for XsdDouble {
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
        assert_eq!(XsdDouble::from(0_f64), XsdDouble::from(0_f64));
        assert_ne!(XsdDouble::NAN, XsdDouble::NAN);
        assert_eq!(XsdDouble::from(-0.), XsdDouble::from(0.));
    }

    #[test]
    fn cmp() {
        assert_eq!(
            XsdDouble::from(0.).partial_cmp(&XsdDouble::from(0.)),
            Some(Ordering::Equal)
        );
        assert_eq!(
            XsdDouble::INFINITY.partial_cmp(&XsdDouble::MAX),
            Some(Ordering::Greater)
        );
        assert_eq!(
            XsdDouble::NEG_INFINITY.partial_cmp(&XsdDouble::MIN),
            Some(Ordering::Less)
        );
        assert_eq!(XsdDouble::NAN.partial_cmp(&XsdDouble::from(0.)), None);
        assert_eq!(XsdDouble::NAN.partial_cmp(&XsdDouble::NAN), None);
        assert_eq!(
            XsdDouble::from(0.).partial_cmp(&XsdDouble::from(-0.)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn is_identical_with() {
        assert!(XsdDouble::from(0.).is_identical_with(XsdDouble::from(0.)));
        assert!(XsdDouble::NAN.is_identical_with(XsdDouble::NAN));
        assert!(!XsdDouble::from(-0.).is_identical_with(XsdDouble::from(0.)));
    }

    #[test]
    fn from_str() -> Result<(), ParseFloatError> {
        assert_eq!(XsdDouble::from_str("NaN")?.to_string(), "NaN");
        assert_eq!(XsdDouble::from_str("INF")?.to_string(), "INF");
        assert_eq!(XsdDouble::from_str("+INF")?.to_string(), "INF");
        assert_eq!(XsdDouble::from_str("-INF")?.to_string(), "-INF");
        assert_eq!(XsdDouble::from_str("0.0E0")?.to_string(), "0");
        assert_eq!(XsdDouble::from_str("-0.0E0")?.to_string(), "-0");
        assert_eq!(XsdDouble::from_str("0.1e1")?.to_string(), "1");
        assert_eq!(XsdDouble::from_str("-0.1e1")?.to_string(), "-1");
        assert_eq!(XsdDouble::from_str("1.e1")?.to_string(), "10");
        assert_eq!(XsdDouble::from_str("-1.e1")?.to_string(), "-10");
        assert_eq!(XsdDouble::from_str("1")?.to_string(), "1");
        assert_eq!(XsdDouble::from_str("-1")?.to_string(), "-1");
        assert_eq!(XsdDouble::from_str("1.")?.to_string(), "1");
        assert_eq!(XsdDouble::from_str("-1.")?.to_string(), "-1");
        assert_eq!(
            XsdDouble::from_str(&f64::MIN.to_string()).unwrap(),
            XsdDouble::MIN
        );
        assert_eq!(
            XsdDouble::from_str(&f64::MAX.to_string()).unwrap(),
            XsdDouble::MAX
        );
        Ok(())
    }
}
