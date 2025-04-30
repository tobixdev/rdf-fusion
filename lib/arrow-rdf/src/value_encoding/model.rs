use datafusion::arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use model::{Decimal, ThinError};
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
            RdfValueEncodingField::Null.name(),
            RdfValueEncodingField::Null.data_type(),
            true,
        ),
        Field::new(
            RdfValueEncodingField::NamedNode.name(),
            RdfValueEncodingField::NamedNode.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::BlankNode.name(),
            RdfValueEncodingField::BlankNode.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::String.name(),
            RdfValueEncodingField::String.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Boolean.name(),
            RdfValueEncodingField::Boolean.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Float.name(),
            RdfValueEncodingField::Float.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Double.name(),
            RdfValueEncodingField::Double.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Decimal.name(),
            RdfValueEncodingField::Decimal.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Int.name(),
            RdfValueEncodingField::Int.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Integer.name(),
            RdfValueEncodingField::Integer.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::DateTime.name(),
            RdfValueEncodingField::DateTime.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Time.name(),
            RdfValueEncodingField::Time.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Date.name(),
            RdfValueEncodingField::Date.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::Duration.name(),
            RdfValueEncodingField::Duration.data_type(),
            false,
        ),
        Field::new(
            RdfValueEncodingField::OtherTypedLiteral.name(),
            RdfValueEncodingField::OtherTypedLiteral.data_type(),
            false,
        ),
    ];

    #[allow(
        clippy::cast_possible_truncation,
        reason = "We know the length of the fields"
    )]
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});

pub struct RdfValueEncoding;

impl RdfValueEncoding {
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

    pub fn data_type() -> DataType {
        DataType::Union(Self::fields().clone(), UnionMode::Dense)
    }
}

#[repr(i8)]
#[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Clone, Copy)]
pub enum RdfValueEncodingField {
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
    OtherTypedLiteral,
}

impl RdfValueEncodingField {
    pub fn type_id(self) -> i8 {
        self.into()
    }

    pub fn name(self) -> &'static str {
        match self {
            RdfValueEncodingField::Null => "null",
            RdfValueEncodingField::NamedNode => "named_node",
            RdfValueEncodingField::BlankNode => "blank_node",
            RdfValueEncodingField::String => "string",
            RdfValueEncodingField::Boolean => "boolean",
            RdfValueEncodingField::Float => "float",
            RdfValueEncodingField::Double => "double",
            RdfValueEncodingField::Decimal => "decimal",
            RdfValueEncodingField::Int => "int",
            RdfValueEncodingField::Integer => "integer",
            RdfValueEncodingField::DateTime => "date_time",
            RdfValueEncodingField::Time => "time",
            RdfValueEncodingField::Date => "date",
            RdfValueEncodingField::Duration => "duration",
            RdfValueEncodingField::OtherTypedLiteral => "other_typed_literal",
        }
    }

    pub fn data_type(self) -> DataType {
        match self {
            RdfValueEncodingField::Null => DataType::Null,
            RdfValueEncodingField::NamedNode | RdfValueEncodingField::BlankNode => DataType::Utf8,
            RdfValueEncodingField::String => DataType::Struct(FIELDS_STRING.clone()),
            RdfValueEncodingField::Boolean => DataType::Boolean,
            RdfValueEncodingField::Float => DataType::Float32,
            RdfValueEncodingField::Double => DataType::Float64,
            RdfValueEncodingField::Decimal => {
                DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE)
            }
            RdfValueEncodingField::Int => DataType::Int32,
            RdfValueEncodingField::Integer => DataType::Int64,
            RdfValueEncodingField::DateTime
            | RdfValueEncodingField::Time
            | RdfValueEncodingField::Date => DataType::Struct(FIELDS_TIMESTAMP.clone()),
            RdfValueEncodingField::Duration => DataType::Struct(FIELDS_DURATION.clone()),
            RdfValueEncodingField::OtherTypedLiteral => {
                DataType::Struct(FIELDS_TYPED_LITERAL.clone())
            }
        }
    }

    pub fn is_literal(self) -> bool {
        matches!(
            self,
            RdfValueEncodingField::NamedNode | RdfValueEncodingField::BlankNode
        )
    }
}

impl Display for RdfValueEncodingField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl TryFrom<i8> for RdfValueEncodingField {
    type Error = ThinError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => RdfValueEncodingField::Null,
            1 => RdfValueEncodingField::NamedNode,
            2 => RdfValueEncodingField::BlankNode,
            3 => RdfValueEncodingField::String,
            4 => RdfValueEncodingField::Boolean,
            5 => RdfValueEncodingField::Float,
            6 => RdfValueEncodingField::Double,
            7 => RdfValueEncodingField::Decimal,
            8 => RdfValueEncodingField::Int,
            9 => RdfValueEncodingField::Integer,
            10 => RdfValueEncodingField::DateTime,
            11 => RdfValueEncodingField::Time,
            12 => RdfValueEncodingField::Date,
            13 => RdfValueEncodingField::Duration,
            14 => RdfValueEncodingField::OtherTypedLiteral,
            _ => return ThinError::internal_error("Unexpected type_id for encoded RDF Term"),
        })
    }
}

impl TryFrom<u8> for RdfValueEncodingField {
    type Error = ThinError;

    #[allow(
        clippy::cast_possible_wrap,
        reason = "Self::try_from will catch any overflow as EncTermField does not have that many variants"
    )]
    fn try_from(value: u8) -> Result<Self, Self::Error> {
        Self::try_from(value as i8)
    }
}

impl From<RdfValueEncodingField> for i8 {
    fn from(value: RdfValueEncodingField) -> Self {
        match value {
            RdfValueEncodingField::Null => 0,
            RdfValueEncodingField::NamedNode => 1,
            RdfValueEncodingField::BlankNode => 2,
            RdfValueEncodingField::String => 3,
            RdfValueEncodingField::Boolean => 4,
            RdfValueEncodingField::Float => 5,
            RdfValueEncodingField::Double => 6,
            RdfValueEncodingField::Decimal => 7,
            RdfValueEncodingField::Int => 8,
            RdfValueEncodingField::Integer => 9,
            RdfValueEncodingField::DateTime => 10,
            RdfValueEncodingField::Time => 11,
            RdfValueEncodingField::Date => 12,
            RdfValueEncodingField::Duration => 13,
            RdfValueEncodingField::OtherTypedLiteral => 14,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        test_type_id(RdfValueEncodingField::NamedNode);
        test_type_id(RdfValueEncodingField::BlankNode);
        test_type_id(RdfValueEncodingField::String);
        test_type_id(RdfValueEncodingField::Boolean);
        test_type_id(RdfValueEncodingField::Float);
        test_type_id(RdfValueEncodingField::Double);
        test_type_id(RdfValueEncodingField::Decimal);
        test_type_id(RdfValueEncodingField::Int);
        test_type_id(RdfValueEncodingField::Integer);
        test_type_id(RdfValueEncodingField::DateTime);
        test_type_id(RdfValueEncodingField::Time);
        test_type_id(RdfValueEncodingField::Date);
        test_type_id(RdfValueEncodingField::Duration);
        test_type_id(RdfValueEncodingField::OtherTypedLiteral);
        test_type_id(RdfValueEncodingField::Null);
    }

    fn test_type_id(term_field: RdfValueEncodingField) {
        assert_eq!(term_field, term_field.type_id().try_into().unwrap());
    }
}
