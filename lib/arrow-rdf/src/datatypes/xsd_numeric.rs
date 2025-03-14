use crate::datatypes::rdf_term::RdfTerm;
use crate::datatypes::xsd_int::XsdInt;
use crate::datatypes::{RdfValue, XsdDecimal, XsdDouble, XsdFloat, XsdInteger};
use crate::encoded::EncTermField;
use crate::DFResult;
use datafusion::arrow::array::UnionArray;
use datafusion::common::{internal_err, ScalarValue};
use std::cmp::Ordering;

#[derive(Copy, Clone, Debug)]
pub enum XsdNumeric {
    Int(XsdInt),
    Integer(XsdInteger),
    Float(XsdFloat),
    Double(XsdDouble),
    Decimal(XsdDecimal),
}

impl XsdNumeric {
    pub fn format_value(&self) -> String {
        match self {
            XsdNumeric::Int(value) => value.to_string(),
            XsdNumeric::Integer(value) => value.to_string(),
            XsdNumeric::Float(value) => value.to_string(),
            XsdNumeric::Double(value) => value.to_string(),
            XsdNumeric::Decimal(value) => value.to_string(),
        }
    }
}

impl PartialEq for XsdNumeric {
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

impl Eq for XsdNumeric {}

impl PartialOrd for XsdNumeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match XsdNumericPair::with_casts_from(self, other) {
            XsdNumericPair::Int(lhs, rhs) => Some(lhs.cmp(&rhs)),
            XsdNumericPair::Integer(lhs, rhs) => Some(lhs.cmp(&rhs)),
            XsdNumericPair::Float(lhs, rhs) => lhs.partial_cmp(&rhs),
            XsdNumericPair::Double(lhs, rhs) => lhs.partial_cmp(&rhs),
            XsdNumericPair::Decimal(lhs, rhs) => Some(lhs.cmp(&rhs)),
        }
    }
}

impl Ord for XsdNumeric {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

pub enum XsdNumericPair {
    Int(XsdInt, XsdInt),
    Integer(XsdInteger, XsdInteger),
    Float(XsdFloat, XsdFloat),
    Double(XsdDouble, XsdDouble),
    Decimal(XsdDecimal, XsdDecimal),
}

impl XsdNumericPair {
    pub fn with_casts_from(lhs: &XsdNumeric, rhs: &XsdNumeric) -> XsdNumericPair {
        match (*lhs, *rhs) {
            (XsdNumeric::Int(lhs), XsdNumeric::Int(rhs)) => XsdNumericPair::Int(lhs, rhs),
            (XsdNumeric::Int(lhs), XsdNumeric::Integer(rhs)) => {
                XsdNumericPair::Integer(lhs.into(), rhs)
            }
            (XsdNumeric::Int(lhs), XsdNumeric::Float(rhs)) => {
                XsdNumericPair::Float(lhs.into(), rhs)
            }
            (XsdNumeric::Int(lhs), XsdNumeric::Double(rhs)) => {
                XsdNumericPair::Double(lhs.into(), rhs)
            }
            (XsdNumeric::Int(lhs), XsdNumeric::Decimal(rhs)) => {
                XsdNumericPair::Decimal(XsdDecimal::from(lhs), rhs)
            }

            (XsdNumeric::Integer(lhs), XsdNumeric::Int(rhs)) => {
                XsdNumericPair::Integer(lhs, rhs.into())
            }
            (XsdNumeric::Integer(lhs), XsdNumeric::Integer(rhs)) => {
                XsdNumericPair::Integer(lhs, rhs)
            }
            (XsdNumeric::Integer(lhs), XsdNumeric::Float(rhs)) => {
                XsdNumericPair::Double(lhs.into(), rhs.into())
            }
            (XsdNumeric::Integer(lhs), XsdNumeric::Double(rhs)) => {
                XsdNumericPair::Double(lhs.into(), rhs)
            }
            (XsdNumeric::Integer(lhs), XsdNumeric::Decimal(rhs)) => {
                XsdNumericPair::Decimal(XsdDecimal::from(lhs), rhs)
            }

            (XsdNumeric::Float(lhs), XsdNumeric::Int(rhs)) => {
                XsdNumericPair::Float(lhs, rhs.into())
            }
            (XsdNumeric::Float(lhs), XsdNumeric::Integer(rhs)) => {
                XsdNumericPair::Double(lhs.into(), rhs.into())
            }
            (XsdNumeric::Float(lhs), XsdNumeric::Float(rhs)) => XsdNumericPair::Float(lhs, rhs),
            (XsdNumeric::Float(lhs), XsdNumeric::Double(rhs)) => {
                XsdNumericPair::Double(lhs.into(), rhs)
            }
            (XsdNumeric::Float(lhs), XsdNumeric::Decimal(rhs)) => {
                XsdNumericPair::Float(lhs, rhs.into())
            }

            (XsdNumeric::Double(lhs), XsdNumeric::Int(rhs)) => {
                XsdNumericPair::Double(lhs, rhs.into())
            }
            (XsdNumeric::Double(lhs), XsdNumeric::Integer(rhs)) => {
                XsdNumericPair::Double(lhs, rhs.into())
            }
            (XsdNumeric::Double(lhs), XsdNumeric::Float(rhs)) => {
                XsdNumericPair::Double(lhs, rhs.into())
            }
            (XsdNumeric::Double(lhs), XsdNumeric::Double(rhs)) => XsdNumericPair::Double(lhs, rhs),
            (XsdNumeric::Double(lhs), XsdNumeric::Decimal(rhs)) => {
                XsdNumericPair::Double(lhs, rhs.into())
            }

            (XsdNumeric::Decimal(lhs), XsdNumeric::Int(rhs)) => {
                XsdNumericPair::Decimal(lhs, rhs.into())
            }
            (XsdNumeric::Decimal(lhs), XsdNumeric::Integer(rhs)) => {
                XsdNumericPair::Decimal(lhs, rhs.into())
            }
            (XsdNumeric::Decimal(lhs), XsdNumeric::Float(rhs)) => {
                XsdNumericPair::Float(lhs.into(), rhs)
            }
            (XsdNumeric::Decimal(lhs), XsdNumeric::Double(rhs)) => {
                XsdNumericPair::Double(lhs.into(), rhs)
            }
            (XsdNumeric::Decimal(lhs), XsdNumeric::Decimal(rhs)) => {
                XsdNumericPair::Decimal(lhs, rhs)
            }
        }
    }
}

impl RdfValue<'_> for XsdNumeric {
    fn from_term(term: RdfTerm<'_>) -> DFResult<Self>
    where
        Self: Sized,
    {
        match term {
            RdfTerm::Numeric(inner) => Ok(inner),
            _ => internal_err!("Unexpected EncRdfTerm"),
        }
    }

    fn from_enc_scalar(scalar: &'_ ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, _)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        let field = EncTermField::try_from(*type_id)?;
        match field {
            EncTermField::Int => Ok(Self::Int(XsdInt::from_enc_scalar(scalar)?)),
            EncTermField::Integer => Ok(Self::Integer(XsdInteger::from_enc_scalar(scalar)?)),
            EncTermField::Float => Ok(Self::Float(XsdFloat::from_enc_scalar(scalar)?)),
            EncTermField::Double => Ok(Self::Double(XsdDouble::from_enc_scalar(scalar)?)),
            EncTermField::Decimal => Ok(Self::Decimal(XsdDecimal::from_enc_scalar(scalar)?)),
            _ => internal_err!("Cannot create EncNumeric from {}.", field),
        }
    }

    fn from_enc_array(array: &UnionArray, index: usize) -> DFResult<Self> {
        let field = EncTermField::try_from(array.type_id(index))?;
        match field {
            EncTermField::Int => Ok(Self::Int(XsdInt::from_enc_array(array, index)?)),
            EncTermField::Integer => Ok(Self::Integer(XsdInteger::from_enc_array(array, index)?)),
            EncTermField::Float => Ok(Self::Float(XsdFloat::from_enc_array(array, index)?)),
            EncTermField::Double => Ok(Self::Double(XsdDouble::from_enc_array(array, index)?)),
            EncTermField::Decimal => Ok(Self::Decimal(XsdDecimal::from_enc_array(array, index)?)),
            _ => internal_err!("Cannot create EncNumeric from {}.", field),
        }
    }
}
