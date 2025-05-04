use crate::xsd::decimal::Decimal;
use crate::xsd::double::Double;
use crate::xsd::float::Float;
use crate::xsd::integer::Integer;
use crate::Int;
use std::cmp::Ordering;
use std::hash::Hash;

#[derive(Copy, Clone, Debug)]
pub enum Numeric {
    Int(Int),
    Integer(Integer),
    Float(Float),
    Double(Double),
    Decimal(Decimal),
}

impl Numeric {
    #[must_use]
    pub fn format_value(&self) -> String {
        match self {
            Numeric::Int(value) => value.to_string(),
            Numeric::Integer(value) => value.to_string(),
            Numeric::Float(value) => value.to_string(),
            Numeric::Double(value) => value.to_string(),
            Numeric::Decimal(value) => value.to_string(),
        }
    }

    #[must_use]
    pub fn to_be_bytes(self) -> Box<[u8]> {
        match self {
            Numeric::Int(int) => int.to_be_bytes().into(),
            Numeric::Integer(int) => int.to_be_bytes().into(),
            Numeric::Float(float) => float.to_be_bytes().into(),
            Numeric::Double(double) => double.to_be_bytes().into(),
            Numeric::Decimal(decimal) => decimal.to_be_bytes().into(),
        }
    }
}

impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(lhs), Self::Int(rhs)) => lhs == rhs,
            (Self::Integer(lhs), Self::Integer(rhs)) => lhs == rhs,
            (Self::Float(lhs), Self::Float(rhs)) => lhs == rhs,
            (Self::Double(lhs), Self::Double(rhs)) => lhs == rhs,
            (Self::Decimal(lhs), Self::Decimal(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl Eq for Numeric {}

impl Hash for Numeric {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Numeric::Int(int) => int.hash(state),
            Numeric::Integer(int) => int.hash(state),
            Numeric::Float(float) => float.to_be_bytes().hash(state),
            Numeric::Double(double) => double.to_be_bytes().hash(state),
            Numeric::Decimal(decimal) => decimal.hash(state),
        }
    }
}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match NumericPair::with_casts_from(*self, *other) {
            NumericPair::Int(lhs, rhs) => Some(lhs.cmp(&rhs)),
            NumericPair::Integer(lhs, rhs) => Some(lhs.cmp(&rhs)),
            NumericPair::Float(lhs, rhs) => lhs.partial_cmp(&rhs),
            NumericPair::Double(lhs, rhs) => lhs.partial_cmp(&rhs),
            NumericPair::Decimal(lhs, rhs) => Some(lhs.cmp(&rhs)),
        }
    }
}

pub enum NumericPair {
    Int(Int, Int),
    Integer(Integer, Integer),
    Float(Float, Float),
    Double(Double, Double),
    Decimal(Decimal, Decimal),
}

impl NumericPair {
    pub fn with_casts_from(lhs: Numeric, rhs: Numeric) -> NumericPair {
        match (lhs, rhs) {
            (Numeric::Int(lhs), Numeric::Int(rhs)) => NumericPair::Int(lhs, rhs),
            (Numeric::Int(lhs), Numeric::Integer(rhs)) => NumericPair::Integer(lhs.into(), rhs),
            (Numeric::Int(lhs), Numeric::Float(rhs)) => NumericPair::Float(lhs.into(), rhs),
            (Numeric::Int(lhs), Numeric::Double(rhs)) => NumericPair::Double(lhs.into(), rhs),
            (Numeric::Int(lhs), Numeric::Decimal(rhs)) => {
                NumericPair::Decimal(Decimal::from(lhs), rhs)
            }

            (Numeric::Integer(lhs), Numeric::Int(rhs)) => NumericPair::Integer(lhs, rhs.into()),
            (Numeric::Integer(lhs), Numeric::Integer(rhs)) => NumericPair::Integer(lhs, rhs),
            (Numeric::Integer(lhs), Numeric::Float(rhs)) => NumericPair::Float(lhs.into(), rhs),
            (Numeric::Integer(lhs), Numeric::Double(rhs)) => NumericPair::Double(lhs.into(), rhs),
            (Numeric::Integer(lhs), Numeric::Decimal(rhs)) => {
                NumericPair::Decimal(Decimal::from(lhs), rhs)
            }

            (Numeric::Float(lhs), Numeric::Int(rhs)) => NumericPair::Float(lhs, rhs.into()),
            (Numeric::Float(lhs), Numeric::Integer(rhs)) => NumericPair::Float(lhs, rhs.into()),
            (Numeric::Float(lhs), Numeric::Float(rhs)) => NumericPair::Float(lhs, rhs),
            (Numeric::Float(lhs), Numeric::Double(rhs)) => NumericPair::Double(lhs.into(), rhs),
            (Numeric::Float(lhs), Numeric::Decimal(rhs)) => NumericPair::Float(lhs, rhs.into()),

            (Numeric::Double(lhs), Numeric::Int(rhs)) => {
                NumericPair::Double(lhs, Integer::from(rhs).into())
            }
            (Numeric::Double(lhs), Numeric::Integer(rhs)) => NumericPair::Double(lhs, rhs.into()),
            (Numeric::Double(lhs), Numeric::Float(rhs)) => NumericPair::Double(lhs, rhs.into()),
            (Numeric::Double(lhs), Numeric::Double(rhs)) => NumericPair::Double(lhs, rhs),
            (Numeric::Double(lhs), Numeric::Decimal(rhs)) => NumericPair::Double(lhs, rhs.into()),

            (Numeric::Decimal(lhs), Numeric::Int(rhs)) => NumericPair::Decimal(lhs, rhs.into()),
            (Numeric::Decimal(lhs), Numeric::Integer(rhs)) => NumericPair::Decimal(lhs, rhs.into()),
            (Numeric::Decimal(lhs), Numeric::Float(rhs)) => NumericPair::Float(lhs.into(), rhs),
            (Numeric::Decimal(lhs), Numeric::Double(rhs)) => NumericPair::Double(lhs.into(), rhs),
            (Numeric::Decimal(lhs), Numeric::Decimal(rhs)) => NumericPair::Decimal(lhs, rhs),
        }
    }
}
