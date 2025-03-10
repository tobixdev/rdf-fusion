use crate::datatypes::{
    RdfTerm, RdfValue, XsdBoolean, XsdDecimal, XsdDouble, XsdFloat, XsdInteger, XsdNumeric,
};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::{AsArray, UnionArray};
use datafusion::arrow::datatypes::Int32Type;
use datafusion::common::{internal_err, ScalarValue};
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

/// [XML Schema `integer` datatype](https://www.w3.org/TR/xmlschema11-2/#integer)
///
/// Uses internally a [`i32`].
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct XsdInt {
    value: i32,
}

impl XsdInt {
    pub const MAX: Self = Self { value: i32::MAX };
    pub const MIN: Self = Self { value: i32::MIN };

    pub fn new(value: i32) -> Self {
        Self { value }
    }

    pub fn as_i32(self) -> i32 {
        self.value
    }

    /// [op:numeric-add](https://www.w3.org/TR/xpath-functions-31/#func-numeric-add)
    ///
    /// Returns `None` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    #[must_use]
    pub fn checked_add(self, rhs: impl Into<Self>) -> Option<Self> {
        Some(Self {
            value: self.value.checked_add(rhs.into().value)?,
        })
    }

    /// [op:numeric-subtract](https://www.w3.org/TR/xpath-functions-31/#func-numeric-subtract)
    ///
    /// Returns `None` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    #[must_use]
    pub fn checked_sub(self, rhs: impl Into<Self>) -> Option<Self> {
        Some(Self {
            value: self.value.checked_sub(rhs.into().value)?,
        })
    }

    /// [op:numeric-multiply](https://www.w3.org/TR/xpath-functions-31/#func-numeric-multiply)
    ///
    /// Returns `None` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    #[must_use]
    pub fn checked_mul(self, rhs: impl Into<Self>) -> Option<Self> {
        Some(Self {
            value: self.value.checked_mul(rhs.into().value)?,
        })
    }

    /// [op:numeric-integer-divide](https://www.w3.org/TR/xpath-functions-31/#func-numeric-integer-divide)
    ///
    /// Returns `None` in case of division by 0 ([FOAR0001](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0001)) or overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    #[must_use]
    pub fn checked_div(self, rhs: impl Into<Self>) -> Option<Self> {
        Some(Self {
            value: self.value.checked_div(rhs.into().value)?,
        })
    }

    /// [op:numeric-mod](https://www.w3.org/TR/xpath-functions-31/#func-numeric-mod)
    ///
    /// Returns `None` in case of division by 0 ([FOAR0001](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0001)) or overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    #[must_use]
    pub fn checked_rem(self, rhs: impl Into<Self>) -> Option<Self> {
        Some(Self {
            value: self.value.checked_rem(rhs.into().value)?,
        })
    }

    /// Euclidean remainder
    ///
    /// Returns `None` in case of division by 0 ([FOAR0001](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0001)) or overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    #[must_use]
    pub fn checked_rem_euclid(self, rhs: impl Into<Self>) -> Option<Self> {
        Some(Self {
            value: self.value.checked_rem_euclid(rhs.into().value)?,
        })
    }

    /// [op:numeric-unary-minus](https://www.w3.org/TR/xpath-functions-31/#func-numeric-unary-minus)
    ///
    /// Returns `None` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    #[must_use]
    pub fn checked_neg(self) -> Option<Self> {
        Some(Self {
            value: self.value.checked_neg()?,
        })
    }

    /// [fn:abs](https://www.w3.org/TR/xpath-functions-31/#func-abs)
    ///
    /// Returns `None` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    #[must_use]
    pub fn checked_abs(self) -> Option<Self> {
        Some(Self {
            value: self.value.checked_abs()?,
        })
    }

    #[inline]
    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.value < 0
    }

    #[inline]
    #[must_use]
    pub const fn is_positive(self) -> bool {
        self.value > 0
    }

    /// Checks if the two values are [identical](https://www.w3.org/TR/xmlschema11-2/#identity).
    #[inline]
    #[must_use]
    pub fn is_identical_with(self, other: Self) -> bool {
        self == other
    }
}

impl RdfValue<'_> for XsdInt {
    fn from_term(term: RdfTerm<'_>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::Numeric(XsdNumeric::Int(inner)) => Ok(inner),
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

        if *type_id != EncTermField::Int.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Int32(Some(value)) => Ok(Self::new(*value)),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_enc_array(array: &UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        let offset = array.value_offset(index);

        match field {
            EncTermField::Int => Ok(Self {
                value: array
                    .child(field.type_id())
                    .as_primitive::<Int32Type>()
                    .value(offset),
            }),
            _ => internal_err!("Cannot create EncBoolean from {}.", field),
        }
    }
}

impl From<bool> for XsdInt {
    #[inline]
    fn from(value: bool) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i8> for XsdInt {
    #[inline]
    fn from(value: i8) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i16> for XsdInt {
    #[inline]
    fn from(value: i16) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i32> for XsdInt {
    #[inline]
    fn from(value: i32) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<u8> for XsdInt {
    #[inline]
    fn from(value: u8) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<u16> for XsdInt {
    #[inline]
    fn from(value: u16) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<XsdBoolean> for XsdInt {
    #[inline]
    fn from(value: XsdBoolean) -> Self {
        bool::from(value).into()
    }
}

impl From<XsdInt> for i32 {
    #[inline]
    fn from(value: XsdInt) -> Self {
        value.value
    }
}

impl FromStr for XsdInt {
    type Err = ParseIntError;

    #[inline]
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(i32::from_str(input)?.into())
    }
}

impl fmt::Display for XsdInt {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl TryFrom<XsdInteger> for XsdInt {
    type Error = TooLargeForIntError;

    #[inline]
    fn try_from(value: XsdInteger) -> Result<Self, Self::Error> {
        XsdDecimal::try_from(value)
            .map_err(|_| TooLargeForIntError)?
            .try_into()
    }
}

impl TryFrom<XsdFloat> for XsdInt {
    type Error = TooLargeForIntError;

    #[inline]
    fn try_from(value: XsdFloat) -> Result<Self, Self::Error> {
        XsdDecimal::try_from(value)
            .map_err(|_| TooLargeForIntError)?
            .try_into()
    }
}

impl TryFrom<XsdDouble> for XsdInt {
    type Error = TooLargeForIntError;

    #[inline]
    fn try_from(value: XsdDouble) -> Result<Self, Self::Error> {
        XsdDecimal::try_from(value)
            .map_err(|_| TooLargeForIntError)?
            .try_into()
    }
}

/// The input is too large to fit into an [`XsdInt`].
///
/// Matches XPath [`FOCA0003` error](https://www.w3.org/TR/xpath-functions-31/#ERRFOCA0003).
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("Value too large for xsd:integer internal representation")]
pub struct TooLargeForIntError;

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;

    #[test]
    fn from_str() -> Result<(), ParseIntError> {
        assert_eq!(XsdInt::from_str("0")?.to_string(), "0");
        assert_eq!(XsdInt::from_str("-0")?.to_string(), "0");
        assert_eq!(XsdInt::from_str("123")?.to_string(), "123");
        assert_eq!(XsdInt::from_str("-123")?.to_string(), "-123");
        XsdInt::from_str("123456789123456789123456789123456789123456789").unwrap_err();
        Ok(())
    }

    #[test]
    fn from_float() -> Result<(), ParseIntError> {
        assert_eq!(
            XsdInt::try_from(XsdFloat::from(0.)).ok(),
            Some(XsdInt::from_str("0")?)
        );
        assert_eq!(
            XsdInt::try_from(XsdFloat::from(-0.)).ok(),
            Some(XsdInt::from_str("0")?)
        );
        assert_eq!(
            XsdInt::try_from(XsdFloat::from(-123.1)).ok(),
            Some(XsdInt::from_str("-123")?)
        );
        XsdInt::try_from(XsdFloat::from(f32::NAN)).unwrap_err();
        XsdInt::try_from(XsdFloat::from(f32::INFINITY)).unwrap_err();
        XsdInt::try_from(XsdFloat::from(f32::NEG_INFINITY)).unwrap_err();
        XsdInt::try_from(XsdFloat::from(f32::MIN)).unwrap_err();
        XsdInt::try_from(XsdFloat::from(f32::MAX)).unwrap_err();
        assert!(
            XsdInt::try_from(XsdFloat::from(1_672_000.))
                .unwrap()
                .checked_sub(XsdInt::from_str("1672000")?)
                .unwrap()
                .checked_abs()
                .unwrap()
                < XsdInt::from(1_000_000)
        );
        Ok(())
    }

    #[test]
    fn from_double() -> Result<(), ParseIntError> {
        assert_eq!(
            XsdInt::try_from(XsdDouble::from(0.0)).ok(),
            Some(XsdInt::from_str("0")?)
        );
        assert_eq!(
            XsdInt::try_from(XsdDouble::from(-0.0)).ok(),
            Some(XsdInt::from_str("0")?)
        );
        assert_eq!(
            XsdInt::try_from(XsdDouble::from(-123.1)).ok(),
            Some(XsdInt::from_str("-123")?)
        );
        assert!(
            XsdInt::try_from(XsdDouble::from(1_672_000.))
                .unwrap()
                .checked_sub(XsdInt::from_str("1672000").unwrap())
                .unwrap()
                .checked_abs()
                .unwrap()
                < XsdInt::from(10)
        );
        XsdInt::try_from(XsdDouble::from(f64::NAN)).unwrap_err();
        XsdInt::try_from(XsdDouble::from(f64::INFINITY)).unwrap_err();
        XsdInt::try_from(XsdDouble::from(f64::NEG_INFINITY)).unwrap_err();
        XsdInt::try_from(XsdDouble::from(f64::MIN)).unwrap_err();
        XsdInt::try_from(XsdDouble::from(f64::MAX)).unwrap_err();
        Ok(())
    }

    #[test]
    fn from_decimal() -> Result<(), ParseIntError> {
        assert_eq!(
            XsdInt::try_from(XsdDecimal::from(0)).ok(),
            Some(XsdInt::from_str("0")?)
        );
        assert_eq!(
            XsdInt::try_from(XsdDecimal::from_str("-123.1").unwrap()).ok(),
            Some(XsdInt::from_str("-123")?)
        );
        XsdInt::try_from(XsdDecimal::MIN).unwrap_err();
        XsdInt::try_from(XsdDecimal::MAX).unwrap_err();
        Ok(())
    }

    #[test]
    fn add() {
        assert_eq!(XsdInt::MIN.checked_add(1), Some(XsdInt::from(i32::MIN + 1)));
        assert_eq!(XsdInt::MAX.checked_add(1), None);
    }

    #[test]
    fn sub() {
        assert_eq!(XsdInt::MIN.checked_sub(1), None);
        assert_eq!(XsdInt::MAX.checked_sub(1), Some(XsdInt::from(i32::MAX - 1)));
    }

    #[test]
    fn mul() {
        assert_eq!(XsdInt::MIN.checked_mul(2), None);
        assert_eq!(XsdInt::MAX.checked_mul(2), None);
    }

    #[test]
    fn div() {
        assert_eq!(XsdInt::from(1).checked_div(0), None);
    }

    #[test]
    fn rem() {
        assert_eq!(XsdInt::from(10).checked_rem(3), Some(XsdInt::from(1)));
        assert_eq!(XsdInt::from(6).checked_rem(-2), Some(XsdInt::from(0)));
        assert_eq!(XsdInt::from(1).checked_rem(0), None);
    }
}
