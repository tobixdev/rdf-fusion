use datafusion::arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
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

const FIELDS_TERM: Lazy<UnionFields> = Lazy::new(|| {
    let fields = vec![
        Field::new(
            DecTermField::NamedNode.name(),
            DecTermField::NamedNode.data_type(),
            true,
        ),
        Field::new(
            DecTermField::BlankNode.name(),
            DecTermField::BlankNode.data_type(),
            true,
        ),
        Field::new(
            DecTermField::String.name(),
            DecTermField::String.data_type(),
            true,
        ),
        Field::new(
            DecTermField::TypedLiteral.name(),
            DecTermField::TypedLiteral.data_type(),
            true,
        ),
    ];
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});

pub struct DecTerm {}

impl DecTerm {
    pub fn term_type_fields() -> UnionFields {
        FIELDS_TERM.clone()
    }

    pub fn string_fields() -> Fields {
        FIELDS_STRING.clone()
    }

    pub fn typed_literal_fields() -> Fields {
        FIELDS_TYPED_LITERAL.clone()
    }

    pub fn term_type() -> DataType {
        DataType::Union(Self::term_type_fields(), UnionMode::Dense)
    }
}

//
// TypeIds
//

#[repr(i8)]
#[derive(Ord, PartialOrd, PartialEq, Eq, Debug, Clone, Copy)]
pub enum DecTermField {
    NamedNode,
    BlankNode,
    String,
    TypedLiteral,
}

impl DecTermField {
    pub fn name(&self) -> &str {
        match self {
            DecTermField::NamedNode => "named_node",
            DecTermField::BlankNode => "blank_node",
            DecTermField::String => "string",
            DecTermField::TypedLiteral => "typed_literal",
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            DecTermField::NamedNode => DataType::Utf8,
            DecTermField::BlankNode => DataType::Utf8,
            DecTermField::String => DataType::Struct(DecTerm::string_fields()),
            DecTermField::TypedLiteral => DataType::Struct(DecTerm::typed_literal_fields()),
        }
    }

    pub fn type_id(&self) -> i8 {
        self.into()
    }
}

impl TryFrom<i8> for DecTermField {
    type Error = DataFusionError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        Ok(match value {
            0 => DecTermField::NamedNode,
            1 => DecTermField::BlankNode,
            2 => DecTermField::String,
            3 => DecTermField::TypedLiteral,
            _ => return exec_err!("Unexpected type_id for decoded RDF Term"),
        })
    }
}

impl From<&DecTermField> for i8 {
    fn from(value: &DecTermField) -> Self {
        match value {
            DecTermField::NamedNode => 0,
            DecTermField::BlankNode => 1,
            DecTermField::String => 2,
            DecTermField::TypedLiteral => 3,
        }
    }
}

impl Display for DecTermField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        test_type_id(DecTermField::NamedNode);
        test_type_id(DecTermField::BlankNode);
        test_type_id(DecTermField::String);
        test_type_id(DecTermField::TypedLiteral);
    }

    fn test_type_id(term_field: DecTermField) {
        assert_eq!(term_field, term_field.type_id().try_into().unwrap());
    }
}
