use crate::plain_term::PLAIN_TERM_ENCODING;
use crate::TermEncoding;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::sync::LazyLock;

/// TODO
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QuadStorageEncoding {
    PlainTerm,
    ObjectId,
}

static PLAIN_TERM_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, PLAIN_TERM_ENCODING.data_type(), true),
        Field::new(COL_SUBJECT, PLAIN_TERM_ENCODING.data_type(), false),
        Field::new(COL_PREDICATE, PLAIN_TERM_ENCODING.data_type(), false),
        Field::new(COL_OBJECT, PLAIN_TERM_ENCODING.data_type(), false),
    ]))
});

static PLAIN_TERM_QUAD_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| DFSchemaRef::new(DFSchema::try_from(PLAIN_TERM_QUAD_SCHEMA.clone()).unwrap()));

static OBJECT_ID_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, PLAIN_TERM_ENCODING.data_type(), true),
        Field::new(COL_SUBJECT, PLAIN_TERM_ENCODING.data_type(), false),
        Field::new(COL_PREDICATE, PLAIN_TERM_ENCODING.data_type(), false),
        Field::new(COL_OBJECT, PLAIN_TERM_ENCODING.data_type(), false),
    ]))
});

static OBJECT_ID_QUAD_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| DFSchemaRef::new(DFSchema::try_from(OBJECT_ID_QUAD_SCHEMA.clone()).unwrap()));

impl QuadStorageEncoding {
    /// TODO
    pub fn quad_schema(self) -> DFSchemaRef {
        match self {
            QuadStorageEncoding::PlainTerm => PLAIN_TERM_QUAD_DFSCHEMA.clone(),
            QuadStorageEncoding::ObjectId => OBJECT_ID_QUAD_DFSCHEMA.clone(),
        }
    }
}
