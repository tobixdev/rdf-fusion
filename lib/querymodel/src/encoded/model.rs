use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::datatypes::{
    DataType, Field, Fields, Schema, SchemaRef, UnionFields, UnionMode,
};
use once_cell::sync::Lazy;
use std::clone::Clone;

pub const ENC_SMALL_STRING_SIZE: usize = 16;
pub const ENC_DICT_KEY_SIZE: usize = 16;
pub const ENC_NUMERICAL_BNODE_SIZE: usize = 16;
pub type EncDictKey = [u8; ENC_DICT_KEY_SIZE];

static ENC_DICT_KEY: Lazy<DataType> =
    Lazy::new(|| DataType::FixedSizeBinary(ENC_DICT_KEY_SIZE as i32));
static ENC_SMALL_STRING: Lazy<DataType> =
    Lazy::new(|| DataType::FixedSizeBinary(ENC_SMALL_STRING_SIZE as i32));

//
// NamedNodes
//

pub static ENC_NAMED_NODE_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::Dictionary(Box::new(ENC_DICT_KEY.clone()), Box::new(DataType::Utf8)));

//
// Blank Nodes
//

pub static ENC_NUMERICAL_BLANK_NODE_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::FixedSizeBinary(ENC_NUMERICAL_BNODE_SIZE as i32));
pub static ENC_SMALL_BLANK_NODE_TYPE: Lazy<DataType> = Lazy::new(|| ENC_SMALL_STRING.clone());
pub static ENC_BIG_BLANK_NODE_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::Dictionary(Box::new(ENC_DICT_KEY.clone()), Box::new(DataType::Utf8)));

//
// Literals
//

pub static ENC_SMALL_STRING_TYPE: Lazy<DataType> = Lazy::new(|| ENC_SMALL_STRING.clone());
pub static ENC_BIG_STRING_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::Dictionary(Box::new(ENC_DICT_KEY.clone()), Box::new(DataType::Utf8)));
pub static ENC_BOOLEAN_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Boolean);
pub static ENC_FLOAT32_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Float32);
pub static ENC_FLOAT64_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Float64);
pub static ENC_INT_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Int32);

pub static ENC_SMALL_TYPED_LITERAL_FIELDS: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("value", ENC_SMALL_STRING_TYPE.clone(), false),
        Field::new("datatype_id", ENC_BIG_STRING_TYPE.clone(), false),
    ])
});
pub static ENC_SMALL_TYPED_LITERAL_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::Struct(ENC_SMALL_TYPED_LITERAL_FIELDS.clone()));

pub static ENC_BIG_TYPED_LITERAL_FIELDS: Lazy<Fields> = Lazy::new(|| {
    Fields::from(vec![
        Field::new("value_id", ENC_DICT_KEY.clone(), false),
        Field::new("datatype_id", ENC_BIG_STRING_TYPE.clone(), false),
    ])
});
pub static ENC_BIG_TYPED_LITERAL_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::Struct(ENC_BIG_TYPED_LITERAL_FIELDS.clone()));

// TODO: Other types

pub const ENC_TERM_FIELD_NAMED_NODE: &str = "named_node";
pub const ENC_TERM_FIELD_SMALL_STRING: &str = "small_string";
pub const ENC_TERM_FIELD_BIG_STRING: &str = "big_string";
pub const ENC_TERM_FIELD_BOOLEAN: &str = "boolean";
pub const ENC_TERM_FIELD_FLOAT32: &str = "float32";
pub const ENC_TERM_FIELD_FLOAT64: &str = "float64";
pub const ENC_TERM_FIELD_INT: &str = "int";
pub const ENC_TERM_FIELD_SMALL_TYPED_LITERAL: &str = "small_typed_literal";
pub const ENC_TERM_FIELD_BIG_TYPED_LITERAL: &str = "big_typed_literal";

pub static ENC_TERM_FIELDS: Lazy<UnionFields> = Lazy::new(|| {
    let fields = vec![
        Field::new(
            ENC_TERM_FIELD_NAMED_NODE,
            ENC_SMALL_STRING_TYPE.clone(),
            true,
        ),
        Field::new(
            ENC_TERM_FIELD_SMALL_STRING,
            ENC_SMALL_STRING_TYPE.clone(),
            true,
        ),
        Field::new(ENC_TERM_FIELD_BIG_STRING, ENC_BIG_STRING_TYPE.clone(), true),
        Field::new(ENC_TERM_FIELD_BOOLEAN, ENC_BOOLEAN_TYPE.clone(), true),
        Field::new(ENC_TERM_FIELD_FLOAT32, ENC_FLOAT32_TYPE.clone(), true),
        Field::new(ENC_TERM_FIELD_FLOAT64, ENC_FLOAT64_TYPE.clone(), true),
        Field::new(ENC_TERM_FIELD_INT, ENC_INT_TYPE.clone(), true),
        Field::new(
            ENC_TERM_FIELD_SMALL_TYPED_LITERAL,
            ENC_SMALL_TYPED_LITERAL_TYPE.clone(),
            true,
        ),
        Field::new(
            ENC_TERM_FIELD_BIG_TYPED_LITERAL,
            ENC_BIG_TYPED_LITERAL_TYPE.clone(),
            true,
        ),
    ];
    UnionFields::new((0..fields.len() as i8).collect::<Vec<_>>(), fields)
});
pub static ENC_TERM_TYPE: Lazy<DataType> =
    Lazy::new(|| DataType::Union(ENC_TERM_FIELDS.clone(), UnionMode::Dense));

//
// Triple Table
//

pub static ENC_QUAD_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, ENC_TERM_TYPE.clone(), false),
        Field::new(COL_SUBJECT, ENC_TERM_TYPE.clone(), false),
        Field::new(COL_PREDICATE, ENC_TERM_TYPE.clone(), false),
        Field::new(COL_OBJECT, ENC_TERM_TYPE.clone(), false),
    ]))
});

//
// TypeIds
//

pub const ENC_TERM_NAMED_NODE_TYPE_ID: Lazy<i8> =
    Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_NAMED_NODE));
pub const ENC_TERM_SMALL_STRING_TYPE_ID: Lazy<i8> =
    Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_SMALL_STRING));
pub const ENC_TERM_BIG_STRING_TYPE_ID: Lazy<i8> =
    Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_BIG_STRING));
pub const ENC_TERM_BOOLEAN_TYPE_ID: Lazy<i8> =
    Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_BOOLEAN));
pub const ENC_TERM_FLOAT32_TYPE_ID: Lazy<i8> =
    Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_FLOAT32));
pub const ENC_TERM_FLOAT64_TYPE_ID: Lazy<i8> =
    Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_FLOAT64));
pub const ENC_TERM_INT_TYPE_ID: Lazy<i8> = Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_INT));
pub const ENC_TERM_BIG_TYPED_LITERAL_TYPE_ID: Lazy<i8> =
    Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_BIG_TYPED_LITERAL));
pub const ENC_TERM_SMALL_TYPED_LITERAL_TYPE_ID: Lazy<i8> =
    Lazy::new(|| field_idx_enc_term(ENC_TERM_FIELD_SMALL_TYPED_LITERAL));

//
// Helper Functions
//

fn field_idx_enc_term(name: &str) -> i8 {
    ENC_TERM_FIELDS
        .iter()
        .filter(|(_, f)| f.name() == name)
        .next()
        .unwrap()
        .0
}
