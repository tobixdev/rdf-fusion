use crate::xsd::decimal::Decimal;
use crate::xsd::double::Double;
use crate::xsd::float::Float;
use crate::xsd::integer::Integer;
use crate::{Int, ThinError, TypedValueRef};
use std::fmt;
use std::str::{FromStr, ParseBoolError};

/// [XML Schema `boolean` datatype](https://www.w3.org/TR/xmlschema11-2/#boolean)
///
/// Uses internally a [`bool`].
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Boolean {
    value: bool,
}

impl Boolean {
    #[inline]
    #[must_use]
    pub fn from_be_bytes(bytes: [u8; 1]) -> Self {
        let value = bytes[0] == 1;
        Self { value }
    }

    #[inline]
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 1] {
        [u8::from(self.value)]
    }

    /// Checks if the two values are [identical](https://www.w3.org/TR/xmlschema11-2/#identity).
    #[inline]
    #[must_use]
    pub fn is_identical_with(self, other: Self) -> bool {
        self == other
    }

    pub fn as_bool(self) -> bool {
        self.value
    }
}

impl From<bool> for Boolean {
    #[inline]
    fn from(value: bool) -> Self {
        Self { value }
    }
}

impl From<Int> for Boolean {
    #[inline]
    fn from(value: Int) -> Self {
        (value != Int::from(0)).into()
    }
}

impl From<Integer> for Boolean {
    #[inline]
    fn from(value: Integer) -> Self {
        (value != Integer::from(0)).into()
    }
}

impl From<Decimal> for Boolean {
    #[inline]
    fn from(value: Decimal) -> Self {
        (value != Decimal::from(0)).into()
    }
}

impl From<Float> for Boolean {
    #[inline]
    fn from(value: Float) -> Self {
        (value != Float::from(0.) && !value.is_nan()).into()
    }
}

impl From<Double> for Boolean {
    #[inline]
    fn from(value: Double) -> Self {
        (value != Double::from(0.) && !value.is_nan()).into()
    }
}

impl From<Boolean> for bool {
    #[inline]
    fn from(value: Boolean) -> Self {
        value.value
    }
}

impl TryFrom<TypedValueRef<'_>> for Boolean {
    type Error = ThinError;

    fn try_from(value: TypedValueRef<'_>) -> Result<Self, Self::Error> {
        match value {
            TypedValueRef::BooleanLiteral(lit) => Ok(lit),
            _ => ThinError::expected(),
        }
    }
}

impl FromStr for Boolean {
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

impl fmt::Display for Boolean {
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
        assert_eq!(Boolean::from_str("true")?.to_string(), "true");
        assert_eq!(Boolean::from_str("1")?.to_string(), "true");
        assert_eq!(Boolean::from_str("false")?.to_string(), "false");
        assert_eq!(Boolean::from_str("0")?.to_string(), "false");
        Ok(())
    }

    #[test]
    fn from_integer() {
        assert_eq!(Boolean::from(false), Integer::from(0).into());
        assert_eq!(Boolean::from(true), Integer::from(1).into());
        assert_eq!(Boolean::from(true), Integer::from(2).into());
    }

    #[test]
    fn from_decimal() {
        assert_eq!(Boolean::from(false), Decimal::from(0).into());
        assert_eq!(Boolean::from(true), Decimal::from(1).into());
        assert_eq!(Boolean::from(true), Decimal::from(2).into());
    }

    #[test]
    fn from_float() {
        assert_eq!(Boolean::from(false), Float::from(0.).into());
        assert_eq!(Boolean::from(true), Float::from(1.).into());
        assert_eq!(Boolean::from(true), Float::from(2.).into());
        assert_eq!(Boolean::from(false), Float::from(f32::NAN).into());
        assert_eq!(Boolean::from(true), Float::from(f32::INFINITY).into());
    }

    #[test]
    fn from_double() {
        assert_eq!(Boolean::from(false), Double::from(0.).into());
        assert_eq!(Boolean::from(true), Double::from(1.).into());
        assert_eq!(Boolean::from(true), Double::from(2.).into());
        assert_eq!(Boolean::from(false), Double::from(f64::NAN).into());
        assert_eq!(Boolean::from(true), Double::from(f64::INFINITY).into());
    }
}
