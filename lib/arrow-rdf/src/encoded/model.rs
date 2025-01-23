use datafusion::arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use datafusion::common::{exec_err, DataFusionError};
use once_cell::unsync::Lazy;
use std::clone::Clone;
use std::fmt::{Display, Formatter};

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

const FIELDS_TYPE: Lazy<UnionFields> = Lazy::new(|| {
    let fields = vec![
        Field::new(
            EncTermField::NamedNode.name(),
            EncTermField::NamedNode.data_type(),
            true,
        ),
        Field::new(
            EncTermField::BlankNode.name(),
            EncTermField::BlankNode.data_type(),
            true,
        ),
        Field::new(
            EncTermField::String.name(),
            EncTermField::String.data_type(),
            true,
        ),
        Field::new(
            EncTermField::Boolean.name(),
            EncTermField::Boolean.data_type(),
            true,
        ),
        Field::new(
            EncTermField::Float32.name(),
            EncTermField::Float32.data_type(),
            true,
        ),
        Field::new(
            EncTermField::Float64.name(),
            EncTermField::Float64.data_type(),
            true,
        ),
        Field::new(
            EncTermField::Int.name(),
            EncTermField::Int.data_type(),
            true,
        ),
        Field::new(
            EncTermField::Integer.name(),
            EncTermField::Integer.data_type(),
            true,
        ),
        Field::new(
            EncTermField::TypedLiteral.name(),
            EncTermField::TypedLiteral.data_type(),
            true,
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
    NamedNode,
    BlankNode,
    String,
    Boolean,
    Float32,
    Float64,
    Int,
    Integer,
    TypedLiteral,
}

impl EncTermField {
    pub fn type_id(&self) -> i8 {
        self.into()
    }

    pub fn name(&self) -> &'static str {
        match self {
            EncTermField::NamedNode => "named_node",
            EncTermField::BlankNode => "blank_node",
            EncTermField::String => "string",
            EncTermField::Boolean => "boolean",
            EncTermField::Float32 => "float32",
            EncTermField::Float64 => "float64",
            EncTermField::Int => "int",
            EncTermField::Integer => "integer",
            EncTermField::TypedLiteral => "typed_literal",
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            EncTermField::NamedNode => DataType::Utf8,
            EncTermField::BlankNode => DataType::Utf8,
            EncTermField::String => DataType::Struct(FIELDS_STRING.clone()),
            EncTermField::Boolean => DataType::Boolean,
            EncTermField::Float32 => DataType::Float32,
            EncTermField::Float64 => DataType::Float64,
            EncTermField::Int => DataType::Int32,
            EncTermField::Integer => DataType::Int64,
            EncTermField::TypedLiteral => DataType::Struct(FIELDS_TYPED_LITERAL.clone()),
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
            0 => EncTermField::NamedNode,
            1 => EncTermField::BlankNode,
            2 => EncTermField::String,
            3 => EncTermField::Boolean,
            4 => EncTermField::Float32,
            5 => EncTermField::Float64,
            6 => EncTermField::Int,
            7 => EncTermField::Integer,
            8 => EncTermField::TypedLiteral,
            _ => return exec_err!("Unexpected type_id for encoded RDF Term"),
        })
    }
}

impl From<&EncTermField> for i8 {
    fn from(value: &EncTermField) -> Self {
        match value {
            EncTermField::NamedNode => 0,
            EncTermField::BlankNode => 1,
            EncTermField::String => 2,
            EncTermField::Boolean => 3,
            EncTermField::Float32 => 4,
            EncTermField::Float64 => 5,
            EncTermField::Int => 6,
            EncTermField::Integer => 7,
            EncTermField::TypedLiteral => 8,
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
        test_type_id(EncTermField::Float32);
        test_type_id(EncTermField::Float64);
        test_type_id(EncTermField::Int);
        test_type_id(EncTermField::Integer);
        test_type_id(EncTermField::TypedLiteral);
    }

    fn test_type_id(term_field: EncTermField) {
        assert_eq!(term_field, term_field.type_id().try_into().unwrap());
    }
}
