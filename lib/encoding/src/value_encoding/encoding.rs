use crate::encoding::TermEncoding;
use crate::value_encoding::array::TermValueArray;
use crate::value_encoding::scalar::TermValueScalar;
use crate::value_encoding::decoders::DefaultTermValueDecoder;
use crate::value_encoding::encoders::DefaultTermValueEncoder;
use crate::DFResult;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use datafusion::common::ScalarValue;
use graphfusion_model::{Decimal, ThinError};
use std::clone::Clone;
use std::fmt::{Display, Formatter};
use std::sync::LazyLock;

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
            ValueEncodingField::Null.name(),
            ValueEncodingField::Null.data_type(),
            true,
        ),
        Field::new(
            ValueEncodingField::NamedNode.name(),
            ValueEncodingField::NamedNode.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::BlankNode.name(),
            ValueEncodingField::BlankNode.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::String.name(),
            ValueEncodingField::String.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Boolean.name(),
            ValueEncodingField::Boolean.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Float.name(),
            ValueEncodingField::Float.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Double.name(),
            ValueEncodingField::Double.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Decimal.name(),
            ValueEncodingField::Decimal.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Int.name(),
            ValueEncodingField::Int.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Integer.name(),
            ValueEncodingField::Integer.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::DateTime.name(),
            ValueEncodingField::DateTime.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Time.name(),
            ValueEncodingField::Time.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Date.name(),
            ValueEncodingField::Date.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::Duration.name(),
            ValueEncodingField::Duration.data_type(),
            false,
        ),
        Field::new(
            ValueEncodingField::OtherLiteral.name(),
            ValueEncodingField::OtherLiteral.data_type(),
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
pub struct TermValueEncoding;

impl TermValueEncoding {
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

impl TermEncoding for TermValueEncoding {
    type Array = TermValueArray;
    type Scalar = TermValueScalar;
    type DefaultEncoder = DefaultTermValueEncoder;
    type DefaultDecoder = DefaultTermValueDecoder;

    fn data_type() -> DataType {
        DataType::Union(Self::fields().clone(), UnionMode::Dense)
    }

    fn try_new_array(array: ArrayRef) -> DFResult<Self::Array> {
        array.try_into()
    }

    fn try_new_scalar(scalar: ScalarValue) -> DFResult<Self::Scalar> {
        scalar.try_into()
    }
}

#[repr(i8)]
#[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Clone, Copy)]
pub enum ValueEncodingField {
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

impl ValueEncodingField {
    pub fn type_id(self) -> i8 {
        self.into()
    }

    pub fn name(self) -> &'static str {
        match self {
            ValueEncodingField::Null => "null",
            ValueEncodingField::NamedNode => "named_node",
            ValueEncodingField::BlankNode => "blank_node",
            ValueEncodingField::String => "string",
            ValueEncodingField::Boolean => "boolean",
            ValueEncodingField::Float => "float",
            ValueEncodingField::Double => "double",
            ValueEncodingField::Decimal => "decimal",
            ValueEncodingField::Int => "int",
            ValueEncodingField::Integer => "integer",
            ValueEncodingField::DateTime => "date_time",
            ValueEncodingField::Time => "time",
            ValueEncodingField::Date => "date",
            ValueEncodingField::Duration => "duration",
            ValueEncodingField::OtherLiteral => "other_literal",
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            ValueEncodingField::Null => DataType::Null,
            ValueEncodingField::NamedNode | ValueEncodingField::BlankNode => DataType::Utf8,
            ValueEncodingField::String => DataType::Struct(FIELDS_STRING.clone()),
            ValueEncodingField::Boolean => DataType::Boolean,
            ValueEncodingField::Float => DataType::Float32,
            ValueEncodingField::Double => DataType::Float64,
            ValueEncodingField::Decimal => DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            ValueEncodingField::Int => DataType::Int32,
            ValueEncodingField::Integer => DataType::Int64,
            ValueEncodingField::DateTime | ValueEncodingField::Time | ValueEncodingField::Date => {
                DataType::Struct(FIELDS_TIMESTAMP.clone())
            }
            ValueEncodingField::Duration => DataType::Struct(FIELDS_DURATION.clone()),
            ValueEncodingField::OtherLiteral => DataType::Struct(FIELDS_TYPED_LITERAL.clone()),
        }
    }

    pub fn is_literal(self) -> bool {
        matches!(
            self,
            ValueEncodingField::NamedNode | ValueEncodingField::BlankNode
        )
    }
}

impl Display for ValueEncodingField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl TryFrom<i8> for ValueEncodingField {
    type Error = ThinError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => ValueEncodingField::Null,
            1 => ValueEncodingField::NamedNode,
            2 => ValueEncodingField::BlankNode,
            3 => ValueEncodingField::String,
            4 => ValueEncodingField::Boolean,
            5 => ValueEncodingField::Float,
            6 => ValueEncodingField::Double,
            7 => ValueEncodingField::Decimal,
            8 => ValueEncodingField::Int,
            9 => ValueEncodingField::Integer,
            10 => ValueEncodingField::DateTime,
            11 => ValueEncodingField::Time,
            12 => ValueEncodingField::Date,
            13 => ValueEncodingField::Duration,
            14 => ValueEncodingField::OtherLiteral,
            _ => return ThinError::internal_error("Unexpected type_id for encoded RDF Term"),
        })
    }
}

impl TryFrom<u8> for ValueEncodingField {
    type Error = ThinError;

    #[allow(
        clippy::cast_possible_wrap,
        reason = "Self::try_from will catch any overflow as EncTermField does not have that many variants"
    )]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_from(value as i8)
    }
}

impl From<ValueEncodingField> for i8 {
    fn from(value: ValueEncodingField) -> Self {
        match value {
            ValueEncodingField::Null => 0,
            ValueEncodingField::NamedNode => 1,
            ValueEncodingField::BlankNode => 2,
            ValueEncodingField::String => 3,
            ValueEncodingField::Boolean => 4,
            ValueEncodingField::Float => 5,
            ValueEncodingField::Double => 6,
            ValueEncodingField::Decimal => 7,
            ValueEncodingField::Int => 8,
            ValueEncodingField::Integer => 9,
            ValueEncodingField::DateTime => 10,
            ValueEncodingField::Time => 11,
            ValueEncodingField::Date => 12,
            ValueEncodingField::Duration => 13,
            ValueEncodingField::OtherLiteral => 14,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        test_type_id(ValueEncodingField::NamedNode);
        test_type_id(ValueEncodingField::BlankNode);
        test_type_id(ValueEncodingField::String);
        test_type_id(ValueEncodingField::Boolean);
        test_type_id(ValueEncodingField::Float);
        test_type_id(ValueEncodingField::Double);
        test_type_id(ValueEncodingField::Decimal);
        test_type_id(ValueEncodingField::Int);
        test_type_id(ValueEncodingField::Integer);
        test_type_id(ValueEncodingField::DateTime);
        test_type_id(ValueEncodingField::Time);
        test_type_id(ValueEncodingField::Date);
        test_type_id(ValueEncodingField::Duration);
        test_type_id(ValueEncodingField::OtherLiteral);
        test_type_id(ValueEncodingField::Null);
    }

    fn test_type_id(term_field: ValueEncodingField) {
        assert_eq!(term_field, term_field.type_id().try_into().unwrap());
    }
}
