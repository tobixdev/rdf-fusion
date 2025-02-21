use crate::encoded::EncTermField;
use crate::{DFResult, RDF_DECIMAL_PRECISION, RDF_DECIMAL_SCALE};
use datafusion::arrow::array::{Array, AsArray, UnionArray};
use datafusion::arrow::datatypes::{
    Decimal128Type, DecimalType, Float32Type, Float64Type, Int32Type, Int64Type,
};
use datafusion::common::{internal_err, ScalarValue};
use oxrdf::vocab::xsd;
use std::cmp::Ordering;
use std::collections::HashSet;

pub trait EncRdfValue<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized;

    fn from_array(array: &'data UnionArray, field: EncTermField, offset: usize) -> DFResult<Self>
    where
        Self: Sized;
}

#[derive(PartialEq, Eq, Debug)]
pub enum EncRdfTerm<'value> {
    NamedNode(EncNamedNode<'value>),
    BlankNode(EncBlankNode<'value>),
    Boolean(EncBoolean),
    Numeric(EncNumeric),
    SimpleLiteral(EncSimpleLiteral<'value>),
    LanguageString(EncLanguageString<'value>),
    TypedLiteral(EncTypedLiteral<'value>),
}

impl<'data> EncRdfValue<'data> for EncRdfTerm<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        match scalar {
            ScalarValue::Union(Some((type_id, value)), _, _) => {
                let type_id = EncTermField::try_from(*type_id)?;
                Ok(match type_id {
                    EncTermField::NamedNode => {
                        EncRdfTerm::NamedNode(EncNamedNode::from_scalar(scalar)?)
                    }
                    EncTermField::BlankNode => {
                        EncRdfTerm::BlankNode(EncBlankNode::from_scalar(scalar)?)
                    }
                    EncTermField::String => match value.as_ref() {
                        ScalarValue::Struct(struct_array) => {
                            match struct_array.column(1).is_null(0) {
                                true => {
                                    EncRdfTerm::SimpleLiteral(EncSimpleLiteral::from_scalar(value)?)
                                }
                                false => EncRdfTerm::LanguageString(
                                    EncLanguageString::from_scalar(value)?,
                                ),
                            }
                        }
                        _ => internal_err!("Unexpected Scalar for String")?,
                    },
                    EncTermField::Boolean => EncRdfTerm::Boolean(EncBoolean::from_scalar(value)?),
                    EncTermField::Float32
                    | EncTermField::Float64
                    | EncTermField::Decimal
                    | EncTermField::Int
                    | EncTermField::Integer => EncRdfTerm::Numeric(EncNumeric::from_scalar(value)?),
                    EncTermField::TypedLiteral => {
                        EncRdfTerm::TypedLiteral(EncTypedLiteral::from_scalar(value)?)
                    }
                })
            }
            _ => internal_err!("Unexpected Scalar"),
        }
    }

    fn from_array(array: &'data UnionArray, field: EncTermField, offset: usize) -> DFResult<Self>
    where
        Self: Sized,
    {
        Ok(match field {
            EncTermField::NamedNode => {
                EncRdfTerm::NamedNode(EncNamedNode::from_array(array, field, offset)?)
            }
            EncTermField::BlankNode => {
                EncRdfTerm::BlankNode(EncBlankNode::from_array(array, field, offset)?)
            }
            EncTermField::String => match array.child(1).is_null(offset) {
                true => {
                    EncRdfTerm::SimpleLiteral(EncSimpleLiteral::from_array(array, field, offset)?)
                }
                false => {
                    EncRdfTerm::LanguageString(EncLanguageString::from_array(array, field, offset)?)
                }
            },
            EncTermField::Boolean => {
                EncRdfTerm::Boolean(EncBoolean::from_array(array, field, offset)?)
            }
            EncTermField::Float32
            | EncTermField::Float64
            | EncTermField::Decimal
            | EncTermField::Int
            | EncTermField::Integer => {
                EncRdfTerm::Numeric(EncNumeric::from_array(array, field, offset)?)
            }
            EncTermField::TypedLiteral => {
                EncRdfTerm::TypedLiteral(EncTypedLiteral::from_array(array, field, offset)?)
            }
        })
    }
}

#[derive(PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct EncNamedNode<'value>(pub &'value str);

impl EncNamedNode<'_> {}

impl<'data> EncRdfValue<'data> for EncNamedNode<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        if *type_id != EncTermField::NamedNode.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self(value.as_str())),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_array(array: &'data UnionArray, field: EncTermField, offset: usize) -> DFResult<Self> {
        match field {
            EncTermField::NamedNode => Ok(EncNamedNode(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => internal_err!("Cannot create EncNamedNode from {}.", field),
        }
    }
}

#[derive(PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct EncBlankNode<'value>(pub &'value str);

impl<'data> EncRdfValue<'data> for EncBlankNode<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        if *type_id != EncTermField::BlankNode.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Utf8(Some(value)) => Ok(Self(value.as_str())),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_array(array: &'data UnionArray, field: EncTermField, offset: usize) -> DFResult<Self> {
        match field {
            EncTermField::BlankNode => Ok(EncBlankNode(
                array
                    .child(field.type_id())
                    .as_string::<i32>()
                    .value(offset),
            )),
            _ => internal_err!("Cannot create EncBlankNode from {}.", field),
        }
    }
}

#[derive(PartialEq, Eq, Debug, PartialOrd, Ord)]
pub struct EncBoolean(pub bool);

impl EncRdfValue<'_> for EncBoolean {
    fn from_scalar(scalar: &'_ ScalarValue) -> DFResult<Self>
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
            ScalarValue::Boolean(Some(value)) => Ok(Self(*value)),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_array(array: &UnionArray, field: EncTermField, offset: usize) -> DFResult<Self> {
        match field {
            EncTermField::Boolean => Ok(Self(
                array.child(field.type_id()).as_boolean().value(offset),
            )),
            _ => internal_err!("Cannot create EncBoolean from {}.", field),
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum EncNumeric {
    I32(i32),
    I64(i64),
    F32(f32),
    F64(f64),
    Decimal(i128),
}

pub(crate) enum EncNumericPair {
    I32(i32, i32),
    I64(i64, i64),
    F32(f32, f32),
    F64(f64, f64),
    Decimal(i128, i128),
}

impl EncNumeric {
    pub fn format_value(&self) -> String {
        match self {
            EncNumeric::I32(value) => value.to_string(),
            EncNumeric::I64(value) => value.to_string(),
            EncNumeric::F32(value) => value.to_string(),
            EncNumeric::F64(value) => value.to_string(),
            EncNumeric::Decimal(value) => {
                Decimal128Type::format_decimal(*value, RDF_DECIMAL_PRECISION, RDF_DECIMAL_SCALE)
            }
        }
    }

    pub fn cast_to_compatible(&self, other: &EncNumeric) -> EncNumericPair {
        match (self, other) {
            (EncNumeric::I32(lhs), EncNumeric::I32(rhs)) => EncNumericPair::I32(*lhs, *rhs),
            (EncNumeric::I32(lhs), EncNumeric::I64(rhs)) => EncNumericPair::I64(*lhs as i64, *rhs),
            (EncNumeric::I32(lhs), EncNumeric::F32(rhs)) => EncNumericPair::F32(*lhs as f32, *rhs),
            (EncNumeric::I32(lhs), EncNumeric::F64(rhs)) => EncNumericPair::F64(*lhs as f64, *rhs),
            (EncNumeric::I32(_lhs), EncNumeric::Decimal(_rhs)) => todo!("Casting to decimal"),

            (EncNumeric::I64(lhs), EncNumeric::I32(rhs)) => EncNumericPair::I64(*lhs, *rhs as i64),
            (EncNumeric::I64(lhs), EncNumeric::I64(rhs)) => EncNumericPair::I64(*lhs, *rhs),
            (EncNumeric::I64(lhs), EncNumeric::F32(rhs)) => {
                EncNumericPair::F64(*lhs as f64, *rhs as f64)
            }
            (EncNumeric::I64(lhs), EncNumeric::F64(rhs)) => EncNumericPair::F64(*lhs as f64, *rhs),
            (EncNumeric::I64(_lhs), EncNumeric::Decimal(_rhs)) => todo!("Casting to decimal"),

            (EncNumeric::F32(lhs), EncNumeric::I32(rhs)) => EncNumericPair::F32(*lhs, *rhs as f32),
            (EncNumeric::F32(lhs), EncNumeric::I64(rhs)) => {
                EncNumericPair::F64(*lhs as f64, *rhs as f64)
            }
            (EncNumeric::F32(lhs), EncNumeric::F32(rhs)) => EncNumericPair::F32(*lhs, *rhs),
            (EncNumeric::F32(lhs), EncNumeric::F64(rhs)) => EncNumericPair::F64(*lhs as f64, *rhs),
            (EncNumeric::F32(_lhs), EncNumeric::Decimal(_rhs)) => todo!("Casting to decimal"),

            (EncNumeric::F64(lhs), EncNumeric::I32(rhs)) => EncNumericPair::F64(*lhs, *rhs as f64),
            (EncNumeric::F64(lhs), EncNumeric::I64(rhs)) => EncNumericPair::F64(*lhs, *rhs as f64),
            (EncNumeric::F64(lhs), EncNumeric::F32(rhs)) => EncNumericPair::F64(*lhs, *rhs as f64),
            (EncNumeric::F64(lhs), EncNumeric::F64(rhs)) => EncNumericPair::F64(*lhs, *rhs),
            (EncNumeric::F64(_lhs), EncNumeric::Decimal(_rhs)) => todo!("Casting to decimal"),

            (EncNumeric::Decimal(_lhs), EncNumeric::I32(_rhs)) => todo!("Casting to decimal"),
            (EncNumeric::Decimal(_lhs), EncNumeric::I64(_rhs)) => todo!("Casting to decimal"),
            (EncNumeric::Decimal(_lhs), EncNumeric::F32(_rhs)) => todo!("Casting to decimal"),
            (EncNumeric::Decimal(_lhs), EncNumeric::F64(_rhs)) => todo!("Casting to decimal"),
            (EncNumeric::Decimal(lhs), EncNumeric::Decimal(rhs)) => {
                EncNumericPair::Decimal(*lhs, *rhs)
            }
        }
    }
}

impl PartialEq for EncNumeric {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::I32(lhs), Self::I32(rhs)) => lhs == rhs,
            (Self::I64(lhs), Self::I64(rhs)) => lhs == rhs,
            (Self::F32(lhs), Self::F32(rhs)) => lhs.total_cmp(rhs) == Ordering::Equal,
            (Self::F64(lhs), Self::F64(rhs)) => lhs.total_cmp(rhs) == Ordering::Equal,
            (Self::Decimal(lhs), Self::Decimal(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl Eq for EncNumeric {}

impl PartialOrd for EncNumeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(match self.cast_to_compatible(other) {
            EncNumericPair::I32(lhs, rhs) => lhs.cmp(&rhs),
            EncNumericPair::I64(lhs, rhs) => lhs.cmp(&rhs),
            EncNumericPair::F32(lhs, rhs) => lhs.total_cmp(&rhs),
            EncNumericPair::F64(lhs, rhs) => lhs.total_cmp(&rhs),
            EncNumericPair::Decimal(lhs, rhs) => lhs.cmp(&rhs),
        })
    }
}

impl Ord for EncNumeric {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

impl EncRdfValue<'_> for EncNumeric {
    fn from_scalar(scalar: &'_ ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        match (EncTermField::try_from(*type_id)?, scalar.as_ref()) {
            (EncTermField::Int, ScalarValue::Int32(Some(value))) => Ok(Self::I32(*value)),
            (EncTermField::Integer, ScalarValue::Int64(Some(value))) => Ok(Self::I64(*value)),
            (EncTermField::Float32, ScalarValue::Float32(Some(value))) => Ok(Self::F32(*value)),
            (EncTermField::Float64, ScalarValue::Float64(Some(value))) => Ok(Self::F64(*value)),
            (
                EncTermField::Decimal,
                ScalarValue::Decimal128(Some(value), RDF_DECIMAL_PRECISION, RDF_DECIMAL_SCALE),
            ) => Ok(Self::Decimal(*value)),
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_array(array: &UnionArray, field: EncTermField, offset: usize) -> DFResult<Self> {
        match field {
            EncTermField::Int => Ok(Self::I32(
                array
                    .child(field.type_id())
                    .as_primitive::<Int32Type>()
                    .value(offset),
            )),
            EncTermField::Integer => Ok(Self::I64(
                array
                    .child(field.type_id())
                    .as_primitive::<Int64Type>()
                    .value(offset),
            )),
            EncTermField::Float32 => Ok(Self::F32(
                array
                    .child(field.type_id())
                    .as_primitive::<Float32Type>()
                    .value(offset),
            )),
            EncTermField::Float64 => Ok(Self::F64(
                array
                    .child(field.type_id())
                    .as_primitive::<Float64Type>()
                    .value(offset),
            )),
            EncTermField::Decimal => Ok(Self::Decimal(
                array
                    .child(field.type_id())
                    .as_primitive::<Decimal128Type>()
                    .value(offset),
            )),
            _ => internal_err!("Cannot create EncNumeric from {}.", field),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct EncSimpleLiteral<'value>(pub &'value str);

impl EncSimpleLiteral<'_> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl PartialOrd for EncSimpleLiteral<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(other.0)
    }
}

impl Ord for EncSimpleLiteral<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

impl<'data> EncRdfValue<'data> for EncSimpleLiteral<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        if *type_id != EncTermField::String.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => Ok(Self(value.column(0).as_string::<i32>().value(0))),
                false => internal_err!("Values has a language"),
            },
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_array(array: &'data UnionArray, field: EncTermField, offset: usize) -> DFResult<Self> {
        match field {
            EncTermField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if !language.is_null(offset) {
                    return internal_err!("Language was not null.");
                }

                Ok(Self(values.value(offset)))
            }
            _ => internal_err!("Cannot create EncSimpleLiteral from {}.", field),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct EncLanguageString<'value>(pub &'value str, pub &'value str);

impl EncLanguageString<'_> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl PartialOrd for EncLanguageString<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(other.0)
    }
}

impl Ord for EncLanguageString<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

impl<'data> EncRdfValue<'data> for EncLanguageString<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        if *type_id != EncTermField::String.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => internal_err!("Values has no language"),
                false => Ok(Self(
                    value.column(0).as_string::<i32>().value(0),
                    value.column(1).as_string::<i32>().value(0),
                )),
            },
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_array(array: &'data UnionArray, field: EncTermField, offset: usize) -> DFResult<Self> {
        match field {
            EncTermField::String => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let language = array.column(1).as_string::<i32>();
                if language.is_null(offset) {
                    return internal_err!("Language was not null.");
                }

                Ok(Self(values.value(offset), language.value(offset)))
            }
            _ => internal_err!("Cannot create EncLanguageString from {}.", field),
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
pub struct EncTypedLiteral<'value>(pub &'value str, pub &'value str);

impl EncTypedLiteral<'_> {
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn is_numeric(&self) -> bool {
        let numeric_types = HashSet::from([
            xsd::INTEGER.as_str(),
            xsd::DECIMAL.as_str(),
            xsd::FLOAT.as_str(),
            xsd::DOUBLE.as_str(),
            xsd::STRING.as_str(),
            xsd::BOOLEAN.as_str(),
            xsd::DATE_TIME.as_str(),
            xsd::NON_POSITIVE_INTEGER.as_str(),
            xsd::NEGATIVE_INTEGER.as_str(),
            xsd::LONG.as_str(),
            xsd::INT.as_str(),
            xsd::SHORT.as_str(),
            xsd::BYTE.as_str(),
            xsd::NON_NEGATIVE_INTEGER.as_str(),
            xsd::UNSIGNED_LONG.as_str(),
            xsd::UNSIGNED_INT.as_str(),
            xsd::UNSIGNED_SHORT.as_str(),
            xsd::UNSIGNED_BYTE.as_str(),
            xsd::POSITIVE_INTEGER.as_str(),
        ]);

        // TODO: We must check whether the literal is valid or encode all numeric types in the union

        numeric_types.contains(self.1)
    }
}

impl PartialOrd for EncTypedLiteral<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.0.partial_cmp(other.0)
    }
}

impl Ord for EncTypedLiteral<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).expect("Ordering is total")
    }
}

impl<'data> EncRdfValue<'data> for EncTypedLiteral<'data> {
    fn from_scalar(scalar: &'data ScalarValue) -> DFResult<Self>
    where
        Self: Sized,
    {
        let ScalarValue::Union(Some((type_id, scalar)), _, _) = scalar else {
            return internal_err!("Unexpected scalar");
        };

        if *type_id != EncTermField::TypedLiteral.type_id() {
            return internal_err!("Unexpected scalar type_id");
        }

        match scalar.as_ref() {
            ScalarValue::Struct(value) => match value.column(1).is_null(0) {
                true => internal_err!("Values has no language"),
                false => Ok(Self(
                    value.column(0).as_string::<i32>().value(0),
                    value.column(1).as_string::<i32>().value(0),
                )),
            },
            _ => internal_err!("Unexpected scalar value"),
        }
    }

    fn from_array(array: &'data UnionArray, field: EncTermField, offset: usize) -> DFResult<Self> {
        match field {
            EncTermField::TypedLiteral => {
                let array = array.child(field.type_id()).as_struct();
                let values = array.column(0).as_string::<i32>();
                let datatypes = array.column(1).as_string::<i32>();
                Ok(Self(values.value(offset), datatypes.value(offset)))
            }
            _ => internal_err!("Cannot create EncTypedLiteral from {}.", field),
        }
    }
}
