use datafusion::arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use datafusion::common::{exec_err, DataFusionError};
use once_cell::unsync::Lazy;
use std::clone::Clone;
use std::fmt::{Display, Formatter};
use datamodel::Decimal;

const FIELDS_STRING: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
    ])
});

const FIELDS_TYPED_LITERAL: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("datatype", DataType::Utf8, false),
    ])
});

const FIELDS_DURATION: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("months", DataType::Int64, true),
        Field::new(
            "seconds",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            true,
        ),
    ])
});

const FIELDS_TYPE: Lazy<UnionFields> = Lazy::new(|| {
    let fields = vec![
        Field::new(
            EncTermField::Null.name(),
            EncTermField::Null.data_type(),
            true,
        ),
        Field::new(
            EncTermField::NamedNode.name(),
            EncTermField::NamedNode.data_type(),
            false,
        ),
        Field::new(
            EncTermField::BlankNode.name(),
            EncTermField::BlankNode.data_type(),
            false,
        ),
        Field::new(
            EncTermField::String.name(),
            EncTermField::String.data_type(),
            false,
        ),
        Field::new(
            EncTermField::Boolean.name(),
            EncTermField::Boolean.data_type(),
            false,
        ),
        Field::new(
            EncTermField::Float.name(),
            EncTermField::Float.data_type(),
            false,
        ),
        Field::new(
            EncTermField::Double.name(),
            EncTermField::Double.data_type(),
            false,
        ),
        Field::new(
            EncTermField::Decimal.name(),
            EncTermField::Decimal.data_type(),
            false,
        ),
        Field::new(
            EncTermField::Int.name(),
            EncTermField::Int.data_type(),
            false,
        ),
        Field::new(
            EncTermField::Integer.name(),
            EncTermField::Integer.data_type(),
            false,
        ),
        Field::new(
            EncTermField::Duration.name(),
            EncTermField::Duration.data_type(),
            false,
        ),
        Field::new(
            EncTermField::TypedLiteral.name(),
            EncTermField::TypedLiteral.data_type(),
            false,
        ),
    ];
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});

pub struct EncTerm {}

impl EncTerm {
    pub fn term_fields() -> UnionFields {
        FIELDS_TYPE.clone()
    }

    pub fn string_fields() -> Fields {
        FIELDS_STRING.clone()
    }

    pub fn string_type() -> DataType {
        DataType::Struct(Self::string_fields())
    }

    pub fn duration_fields() -> Fields {
        FIELDS_DURATION.clone()
    }

    pub fn typed_literal_fields() -> Fields {
        FIELDS_TYPED_LITERAL.clone()
    }

    pub fn term_type() -> DataType {
        DataType::Union(Self::term_fields().clone(), UnionMode::Dense)
    }
}

#[repr(i8)]
#[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Clone, Copy)]
pub enum EncTermField {
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
    Duration,
    TypedLiteral,
}

impl EncTermField {
    pub fn type_id(&self) -> i8 {
        self.into()
    }

    pub fn name(&self) -> &'static str {
        match self {
            EncTermField::Null => "null",
            EncTermField::NamedNode => "named_node",
            EncTermField::BlankNode => "blank_node",
            EncTermField::String => "string",
            EncTermField::Boolean => "boolean",
            EncTermField::Float => "float",
            EncTermField::Double => "double",
            EncTermField::Decimal => "decimal",
            EncTermField::Int => "int",
            EncTermField::Integer => "integer",
            EncTermField::Duration => "duration",
            EncTermField::TypedLiteral => "typed_literal",
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            EncTermField::Null => DataType::Null,
            EncTermField::NamedNode => DataType::Utf8,
            EncTermField::BlankNode => DataType::Utf8,
            EncTermField::String => DataType::Struct(FIELDS_STRING.clone()),
            EncTermField::Boolean => DataType::Boolean,
            EncTermField::Float => DataType::Float32,
            EncTermField::Double => DataType::Float64,
            EncTermField::Decimal => DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            EncTermField::Int => DataType::Int32,
            EncTermField::Integer => DataType::Int64,
            EncTermField::Duration => DataType::Struct(FIELDS_DURATION.clone()),
            EncTermField::TypedLiteral => DataType::Struct(FIELDS_TYPED_LITERAL.clone()),
        }
    }

    pub fn is_literal(&self) -> bool {
        match self {
            EncTermField::NamedNode | EncTermField::BlankNode => false,
            _ => true,
        }
    }
}

impl Display for EncTermField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

impl TryFrom<i8> for EncTermField {
    type Error = DataFusionError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => EncTermField::Null,
            1 => EncTermField::NamedNode,
            2 => EncTermField::BlankNode,
            3 => EncTermField::String,
            4 => EncTermField::Boolean,
            5 => EncTermField::Float,
            6 => EncTermField::Double,
            7 => EncTermField::Decimal,
            8 => EncTermField::Int,
            9 => EncTermField::Integer,
            10 => EncTermField::Duration,
            11 => EncTermField::TypedLiteral,
            _ => return exec_err!("Unexpected type_id for encoded RDF Term"),
        })
    }
}

impl From<&EncTermField> for i8 {
    fn from(value: &EncTermField) -> Self {
        match value {
            EncTermField::Null => 0,
            EncTermField::NamedNode => 1,
            EncTermField::BlankNode => 2,
            EncTermField::String => 3,
            EncTermField::Boolean => 4,
            EncTermField::Float => 5,
            EncTermField::Double => 6,
            EncTermField::Decimal => 7,
            EncTermField::Int => 8,
            EncTermField::Integer => 9,
            EncTermField::Duration => 10,
            EncTermField::TypedLiteral => 11,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        test_type_id(EncTermField::NamedNode);
        test_type_id(EncTermField::BlankNode);
        test_type_id(EncTermField::String);
        test_type_id(EncTermField::Boolean);
        test_type_id(EncTermField::Float);
        test_type_id(EncTermField::Double);
        test_type_id(EncTermField::Decimal);
        test_type_id(EncTermField::Int);
        test_type_id(EncTermField::Integer);
        test_type_id(EncTermField::Duration);
        test_type_id(EncTermField::TypedLiteral);
        test_type_id(EncTermField::Null);
    }

    fn test_type_id(term_field: EncTermField) {
        assert_eq!(term_field, term_field.type_id().try_into().unwrap());
    }
}
