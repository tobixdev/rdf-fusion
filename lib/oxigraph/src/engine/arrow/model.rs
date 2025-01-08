use datafusion::arrow::datatypes::{
    DataType, Field, Schema, SchemaRef,
};
use once_cell::sync::Lazy;

pub static IRI_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);
pub static BNODE_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);
pub static SUBJECT_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);
pub static RDF_TERM_TYPE: Lazy<DataType> = Lazy::new(|| DataType::Utf8);

pub static QUAD_TABLE_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new("graph", IRI_TYPE.clone(), false),
        Field::new("subject", SUBJECT_TYPE.clone(), false),
        Field::new("predicate", IRI_TYPE.clone(), false),
        Field::new("object", RDF_TERM_TYPE.clone(), false),
    ]))
});
