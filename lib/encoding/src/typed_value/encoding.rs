use crate::encoding::TermEncoding;
use crate::typed_value::array::TermValueArray;
use crate::typed_value::scalar::TermValueScalar;
use crate::{DFResult, EncodingName, TermEncoder};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use datafusion::common::ScalarValue;
use rdf_fusion_model::{Decimal, TermRef, ThinError};
use std::clone::Clone;
use std::fmt::{Display, Formatter};
use std::sync::LazyLock;
use crate::typed_value::encoders::TermRefTypedValueEncoder;

static FIELDS_STRING: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
    ])
});

static FIELDS_TIMESTAMP: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new(
            "value",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            false,
        ),
        Field::new("offset", DataType::Int16, true),
    ])
});

static FIELDS_TYPED_LITERAL: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("datatype", DataType::Utf8, false),
    ])
});

static FIELDS_DURATION: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("months", DataType::Int64, true),
        Field::new(
            "seconds",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            true,
        ),
    ])
});

static FIELDS_TYPE: LazyLock<UnionFields> = LazyLock::new(|| {
    let fields = vec![
        Field::new(
            TypedValueEncodingField::Null.name(),
            TypedValueEncodingField::Null.data_type(),
            true,
        ),
        Field::new(
            TypedValueEncodingField::NamedNode.name(),
            TypedValueEncodingField::NamedNode.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::BlankNode.name(),
            TypedValueEncodingField::BlankNode.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::String.name(),
            TypedValueEncodingField::String.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Boolean.name(),
            TypedValueEncodingField::Boolean.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Float.name(),
            TypedValueEncodingField::Float.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Double.name(),
            TypedValueEncodingField::Double.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Decimal.name(),
            TypedValueEncodingField::Decimal.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Int.name(),
            TypedValueEncodingField::Int.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Integer.name(),
            TypedValueEncodingField::Integer.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::DateTime.name(),
            TypedValueEncodingField::DateTime.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Time.name(),
            TypedValueEncodingField::Time.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Date.name(),
            TypedValueEncodingField::Date.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::Duration.name(),
            TypedValueEncodingField::Duration.data_type(),
            false,
        ),
        Field::new(
            TypedValueEncodingField::OtherLiteral.name(),
            TypedValueEncodingField::OtherLiteral.data_type(),
            false,
        ),
    ];

    #[allow(
        clippy::cast_possible_truncation,
        reason = "We know the length of the fields"
    )]
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});

/// TODO
#[derive(Debug)]
pub struct TypedValueEncoding;

impl TypedValueEncoding {
    pub fn fields() -> UnionFields {
        FIELDS_TYPE.clone()
    }

    pub fn string_fields() -> Fields {
        FIELDS_STRING.clone()
    }

    pub fn string_type() -> DataType {
        DataType::Struct(Self::string_fields())
    }

    pub fn timestamp_fields() -> Fields {
        FIELDS_TIMESTAMP.clone()
    }

    pub fn duration_fields() -> Fields {
        FIELDS_DURATION.clone()
    }

    pub fn typed_literal_fields() -> Fields {
        FIELDS_TYPED_LITERAL.clone()
    }
}

impl TermEncoding for TypedValueEncoding {
    type Array = TermValueArray;
    type Scalar = TermValueScalar;

    fn name() -> EncodingName {
        EncodingName::TypedValue
    }

    fn data_type() -> DataType {
        DataType::Union(Self::fields().clone(), UnionMode::Dense)
    }

    fn try_new_array(array: ArrayRef) -> DFResult<Self::Array> {
        array.try_into()
    }

    fn try_new_scalar(scalar: ScalarValue) -> DFResult<Self::Scalar> {
        scalar.try_into()
    }

    fn encode_scalar(term: TermRef<'_>) -> DFResult<Self::Scalar> {
        TermRefTypedValueEncoder::encode_term(Ok(term))
    }

    fn encode_null_scalar() -> DFResult<Self::Scalar> {
        TermRefTypedValueEncoder::encode_term(ThinError::expected())
    }
}

#[repr(i8)]
#[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Clone, Copy)]
pub enum TypedValueEncodingField {
    /// Represents an unbound value or an error.
    ///
    /// This has to be the first encoded field as OUTER joins will use it to initialize default
    /// values for non-matching rows.
    Null,
    NamedNode,
    BlankNode,
    String,
    Boolean,
    Float,
    Double,
    Decimal,
    Int,
    Integer,
    DateTime,
    Time,
    Date,
    Duration,
    OtherLiteral,
}

impl TypedValueEncodingField {
    pub fn type_id(self) -> i8 {
        self.into()
    }

    pub fn name(self) -> &'static str {
        match self {
            TypedValueEncodingField::Null => "null",
            TypedValueEncodingField::NamedNode => "named_node",
            TypedValueEncodingField::BlankNode => "blank_node",
            TypedValueEncodingField::String => "string",
            TypedValueEncodingField::Boolean => "boolean",
            TypedValueEncodingField::Float => "float",
            TypedValueEncodingField::Double => "double",
            TypedValueEncodingField::Decimal => "decimal",
            TypedValueEncodingField::Int => "int",
            TypedValueEncodingField::Integer => "integer",
            TypedValueEncodingField::DateTime => "date_time",
            TypedValueEncodingField::Time => "time",
            TypedValueEncodingField::Date => "date",
            TypedValueEncodingField::Duration => "duration",
            TypedValueEncodingField::OtherLiteral => "other_literal",
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            TypedValueEncodingField::Null => DataType::Null,
            TypedValueEncodingField::NamedNode | TypedValueEncodingField::BlankNode => {
                DataType::Utf8
            }
            TypedValueEncodingField::String => DataType::Struct(FIELDS_STRING.clone()),
            TypedValueEncodingField::Boolean => DataType::Boolean,
            TypedValueEncodingField::Float => DataType::Float32,
            TypedValueEncodingField::Double => DataType::Float64,
            TypedValueEncodingField::Decimal => {
                DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE)
            }
            TypedValueEncodingField::Int => DataType::Int32,
            TypedValueEncodingField::Integer => DataType::Int64,
            TypedValueEncodingField::DateTime
            | TypedValueEncodingField::Time
            | TypedValueEncodingField::Date => DataType::Struct(FIELDS_TIMESTAMP.clone()),
            TypedValueEncodingField::Duration => DataType::Struct(FIELDS_DURATION.clone()),
            TypedValueEncodingField::OtherLiteral => DataType::Struct(FIELDS_TYPED_LITERAL.clone()),
        }
    }

    pub fn is_literal(self) -> bool {
        matches!(
            self,
            TypedValueEncodingField::NamedNode | TypedValueEncodingField::BlankNode
        )
    }
}

impl Display for TypedValueEncodingField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl TryFrom<i8> for TypedValueEncodingField {
    type Error = ThinError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => TypedValueEncodingField::Null,
            1 => TypedValueEncodingField::NamedNode,
            2 => TypedValueEncodingField::BlankNode,
            3 => TypedValueEncodingField::String,
            4 => TypedValueEncodingField::Boolean,
            5 => TypedValueEncodingField::Float,
            6 => TypedValueEncodingField::Double,
            7 => TypedValueEncodingField::Decimal,
            8 => TypedValueEncodingField::Int,
            9 => TypedValueEncodingField::Integer,
            10 => TypedValueEncodingField::DateTime,
            11 => TypedValueEncodingField::Time,
            12 => TypedValueEncodingField::Date,
            13 => TypedValueEncodingField::Duration,
            14 => TypedValueEncodingField::OtherLiteral,
            _ => return ThinError::internal_error("Unexpected type_id for encoded RDF Term"),
        })
    }
}

impl TryFrom<u8> for TypedValueEncodingField {
    type Error = ThinError;

    #[allow(
        clippy::cast_possible_wrap,
        reason = "Self::try_from will catch any overflow as EncTermField does not have that many variants"
    )]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_from(value as i8)
    }
}

impl From<TypedValueEncodingField> for i8 {
    fn from(value: TypedValueEncodingField) -> Self {
        match value {
            TypedValueEncodingField::Null => 0,
            TypedValueEncodingField::NamedNode => 1,
            TypedValueEncodingField::BlankNode => 2,
            TypedValueEncodingField::String => 3,
            TypedValueEncodingField::Boolean => 4,
            TypedValueEncodingField::Float => 5,
            TypedValueEncodingField::Double => 6,
            TypedValueEncodingField::Decimal => 7,
            TypedValueEncodingField::Int => 8,
            TypedValueEncodingField::Integer => 9,
            TypedValueEncodingField::DateTime => 10,
            TypedValueEncodingField::Time => 11,
            TypedValueEncodingField::Date => 12,
            TypedValueEncodingField::Duration => 13,
            TypedValueEncodingField::OtherLiteral => 14,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        test_type_id(TypedValueEncodingField::NamedNode);
        test_type_id(TypedValueEncodingField::BlankNode);
        test_type_id(TypedValueEncodingField::String);
        test_type_id(TypedValueEncodingField::Boolean);
        test_type_id(TypedValueEncodingField::Float);
        test_type_id(TypedValueEncodingField::Double);
        test_type_id(TypedValueEncodingField::Decimal);
        test_type_id(TypedValueEncodingField::Int);
        test_type_id(TypedValueEncodingField::Integer);
        test_type_id(TypedValueEncodingField::DateTime);
        test_type_id(TypedValueEncodingField::Time);
        test_type_id(TypedValueEncodingField::Date);
        test_type_id(TypedValueEncodingField::Duration);
        test_type_id(TypedValueEncodingField::OtherLiteral);
        test_type_id(TypedValueEncodingField::Null);
    }

    fn test_type_id(term_field: TypedValueEncodingField) {
        assert_eq!(term_field, term_field.type_id().try_into().unwrap());
    }
}
