use datafusion::arrow::datatypes::{DataType, Field, Fields, UnionFields, UnionMode};
use once_cell::unsync::Lazy;
use std::clone::Clone;
//
// NamedNodes
//

pub const ENC_TYPE_NAMED_NODE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);

//
// Blank Nodes
//

pub const ENC_TYPE_BLANK_NODE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);

//
// Literals
//

pub const ENC_FIELDS_STRING: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
    ])
});
pub const ENC_TYPE_STRING: Lazy<DataType> =
    Lazy::new(|| DataType::Struct(ENC_FIELDS_STRING.clone()));
pub const ENC_TYPE_BOOLEAN: Lazy<DataType> = Lazy::new(|| DataType::Boolean);
pub const ENC_TYPE_FLOAT32: Lazy<DataType> = Lazy::new(|| DataType::Float32);
pub const ENC_TYPE_FLOAT64: Lazy<DataType> = Lazy::new(|| DataType::Float64);
pub const ENC_TYPE_INT: Lazy<DataType> = Lazy::new(|| DataType::Int32);
pub const ENC_TYPE_INTEGER: Lazy<DataType> = Lazy::new(|| DataType::Int64); // TODO: Big Int

pub const ENC_FIELDS_TYPED_LITERAL: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("datatype", DataType::Utf8, false),
    ])
});
pub const ENC_TYPE_TYPED_LITERAL: Lazy<DataType> =
    Lazy::new(|| DataType::Struct(ENC_FIELDS_TYPED_LITERAL.clone()));

// TODO: Other types

pub const ENC_FIELD_NAMED_NODE: &str = "named_node";
pub const ENC_FIELD_BLANK_NODE: &str = "blank_node";
pub const ENC_FIELD_STRING: &str = "string";
pub const ENC_FIELD_BOOLEAN: &str = "boolean";
pub const ENC_FIELD_FLOAT32: &str = "float32";
pub const ENC_FIELD_FLOAT64: &str = "float64";
pub const ENC_FIELD_INT: &str = "int";
pub const ENC_FIELD_INTEGER: &str = "integer";
pub const ENC_FIELD_TYPED_LITERAL: &str = "typed_literal";

pub const ENC_FIELDS_TERM: Lazy<UnionFields> = Lazy::new(|| {
    let fields = vec![
        Field::new(ENC_FIELD_NAMED_NODE, ENC_TYPE_NAMED_NODE.clone(), true),
        Field::new(ENC_FIELD_BLANK_NODE, ENC_TYPE_BLANK_NODE.clone(), true),
        Field::new(ENC_FIELD_STRING, ENC_TYPE_STRING.clone(), true),
        Field::new(ENC_FIELD_BOOLEAN, ENC_TYPE_BOOLEAN.clone(), true),
        Field::new(ENC_FIELD_FLOAT32, ENC_TYPE_FLOAT32.clone(), true),
        Field::new(ENC_FIELD_FLOAT64, ENC_TYPE_FLOAT64.clone(), true),
        Field::new(ENC_FIELD_INT, ENC_TYPE_INT.clone(), true),
        Field::new(ENC_FIELD_INTEGER, ENC_TYPE_INTEGER.clone(), true),
        Field::new(
            ENC_FIELD_TYPED_LITERAL,
            ENC_TYPE_TYPED_LITERAL.clone(),
            true,
        ),
    ];
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});
pub const ENC_TYPE_TERM: Lazy<DataType> =
    Lazy::new(|| DataType::Union(ENC_FIELDS_TERM.clone(), UnionMode::Dense));

//
// TypeIds
//

pub const ENC_TYPE_ID_NAMED_NODE: i8 = 0;
pub const ENC_TYPE_ID_BLANK_NODE: i8 = 1;
pub const ENC_TYPE_ID_STRING: i8 = 2;
pub const ENC_TYPE_ID_BOOLEAN: i8 = 3;
pub const ENC_TYPE_ID_FLOAT32: i8 = 4;
pub const ENC_TYPE_ID_FLOAT64: i8 = 5;
pub const ENC_TYPE_ID_INT: i8 = 6;
pub const ENC_TYPE_ID_INTEGER: i8 = 7;
pub const ENC_TYPE_ID_TYPED_LITERAL: i8 = 8;

//
// Helper Functions
//

pub fn enc_field_idx_term(name: &str) -> i8 {
    ENC_FIELDS_TERM
        .iter()
        .filter(|(_, f)| f.name() == name)
        .next()
        .unwrap()
        .0
}

pub fn enc_idx_to_field_name(idx: i8) -> String {
    ENC_FIELDS_TERM
        .iter()
        .nth(idx as usize)
        .map(|f| f.1.name().clone())
        .unwrap()
}

pub fn enc_is_nested_rdf_term(idx: i8) -> bool {
    idx == ENC_TYPE_ID_STRING || idx == ENC_TYPE_ID_TYPED_LITERAL
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_type_ids() {
        assert_eq!(
            ENC_TYPE_ID_NAMED_NODE,
            enc_field_idx_term(ENC_FIELD_NAMED_NODE)
        );
        assert_eq!(
            ENC_TYPE_ID_BLANK_NODE,
            enc_field_idx_term(ENC_FIELD_BLANK_NODE)
        );
        assert_eq!(ENC_TYPE_ID_STRING, enc_field_idx_term(ENC_FIELD_STRING));
        assert_eq!(ENC_TYPE_ID_BOOLEAN, enc_field_idx_term(ENC_FIELD_BOOLEAN));
        assert_eq!(ENC_TYPE_ID_FLOAT32, enc_field_idx_term(ENC_FIELD_FLOAT32));
        assert_eq!(ENC_TYPE_ID_FLOAT64, enc_field_idx_term(ENC_FIELD_FLOAT64));
        assert_eq!(ENC_TYPE_ID_INT, enc_field_idx_term(ENC_FIELD_INT));
        assert_eq!(ENC_TYPE_ID_INTEGER, enc_field_idx_term(ENC_FIELD_INTEGER));
        assert_eq!(
            ENC_TYPE_ID_TYPED_LITERAL,
            enc_field_idx_term(ENC_FIELD_TYPED_LITERAL)
        );
    }
}
