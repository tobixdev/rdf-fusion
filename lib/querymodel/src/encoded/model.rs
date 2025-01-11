use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema, SchemaRef, UnionFields, UnionMode,
};
use once_cell::unsync::Lazy;
use std::clone::Clone;
//
// NamedNodes
//

pub const TYPE_NAMED_NODE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);

//
// Blank Nodes
//

pub const TYPE_BLANK_NODE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);

//
// Literals
//

pub const FIELDS_STRING: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("language", DataType::Utf8, true),
    ])
});
pub const TYPE_STRING: Lazy<DataType> = Lazy::new(|| DataType::Struct(FIELDS_STRING.clone()));
pub const TYPE_BOOLEAN: Lazy<DataType> = Lazy::new(|| DataType::Boolean);
pub const TYPE_FLOAT32: Lazy<DataType> = Lazy::new(|| DataType::Float32);
pub const TYPE_FLOAT64: Lazy<DataType> = Lazy::new(|| DataType::Float64);
pub const TYPE_INT: Lazy<DataType> = Lazy::new(|| DataType::Int32);
pub const TYPE_INTEGER: Lazy<DataType> = Lazy::new(|| DataType::Int64); // TODO: Big Int

pub const FIELDS_TYPED_LITERAL: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("datatype", DataType::Utf8, false),
    ])
});
pub const TYPE_TYPED_LITERAL: Lazy<DataType> =
    Lazy::new(|| DataType::Struct(FIELDS_TYPED_LITERAL.clone()));

// TODO: Other types

pub const FIELD_NAMED_NODE: &str = "named_node";
pub const FIELD_BLANK_NODE: &str = "blank_node";
pub const FIELD_STRING: &str = "string";
pub const FIELD_BOOLEAN: &str = "boolean";
pub const FIELD_FLOAT32: &str = "float32";
pub const FIELD_FLOAT64: &str = "float64";
pub const FIELD_INT: &str = "int";
pub const FIELD_INTEGER: &str = "integer";
pub const FIELD_TYPED_LITERAL: &str = "typed_literal";

pub const FIELDS_TERM: Lazy<UnionFields> = Lazy::new(|| {
    let fields = vec![
        Field::new(FIELD_NAMED_NODE, TYPE_NAMED_NODE.clone(), true),
        Field::new(FIELD_BLANK_NODE, TYPE_BLANK_NODE.clone(), true),
        Field::new(FIELD_STRING, TYPE_STRING.clone(), true),
        Field::new(FIELD_BOOLEAN, TYPE_BOOLEAN.clone(), true),
        Field::new(FIELD_FLOAT32, TYPE_FLOAT32.clone(), true),
        Field::new(FIELD_FLOAT64, TYPE_FLOAT64.clone(), true),
        Field::new(FIELD_INT, TYPE_INT.clone(), true),
        Field::new(FIELD_INTEGER, TYPE_INTEGER.clone(), true),
        Field::new(FIELD_TYPED_LITERAL, TYPE_TYPED_LITERAL.clone(), true),
    ];
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});
pub const TYPE_TERM: Lazy<DataType> =
    Lazy::new(|| DataType::Union(FIELDS_TERM.clone(), UnionMode::Dense));

//
// Triple Table
//

pub const QUAD_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, TYPE_TERM.clone(), false),
        Field::new(COL_SUBJECT, TYPE_TERM.clone(), false),
        Field::new(COL_PREDICATE, TYPE_TERM.clone(), false),
        Field::new(COL_OBJECT, TYPE_TERM.clone(), false),
    ]))
});

//
// TypeIds
//

pub const TYPE_ID_NAMED_NODE: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_NAMED_NODE));
pub const TYPE_ID_BLANK_NODE: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_BLANK_NODE));
pub const TYPE_ID_STRING: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_STRING));
pub const TYPE_ID_BOOLEAN: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_BOOLEAN));
pub const TYPE_ID_FLOAT32: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_FLOAT32));
pub const TYPE_ID_FLOAT64: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_FLOAT64));
pub const TYPE_ID_INT: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_INT));
pub const TYPE_ID_INTEGER: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_INTEGER));
pub const TYPE_ID_TYPED_LITERAL: Lazy<i8> = Lazy::new(|| field_idx_enc_term(FIELD_TYPED_LITERAL));

//
// Helper Functions
//

pub fn field_idx_enc_term(name: &str) -> i8 {
    FIELDS_TERM
        .iter()
        .filter(|(_, f)| f.name() == name)
        .next()
        .unwrap()
        .0
}

pub fn idx_to_field_name(idx: i8) -> String {
    FIELDS_TERM
        .iter()
        .nth(idx as usize)
        .map(|f| f.1.name().clone())
        .unwrap()
}

pub fn is_nested_rdf_term(idx: i8) -> bool {
    idx == *TYPE_ID_STRING || idx == *TYPE_ID_TYPED_LITERAL
}
