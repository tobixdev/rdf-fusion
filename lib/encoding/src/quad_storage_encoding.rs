use crate::object_id::ObjectIdEncoding;
use crate::plain_term::PLAIN_TERM_ENCODING;
use crate::TermEncoding;
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

/// TODO
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QuadStorageEncoding {
    PlainTerm,
    ObjectId(ObjectIdEncoding),
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

impl QuadStorageEncoding {
    /// TODO
    pub fn term_type(&self) -> DataType {
        match self {
            QuadStorageEncoding::PlainTerm => PLAIN_TERM_ENCODING.data_type(),
            QuadStorageEncoding::ObjectId(enc) => enc.data_type(),
        }
    }

    /// TODO
    pub fn quad_schema(&self) -> DFSchemaRef {
        match self {
            QuadStorageEncoding::PlainTerm => PLAIN_TERM_QUAD_DFSCHEMA.clone(),
            QuadStorageEncoding::ObjectId(encoding) => object_id_quad_schema(encoding),
        }
    }

    /// TODO
    pub fn object_id_encoding(&self) -> Option<&ObjectIdEncoding> {
        match &self {
            QuadStorageEncoding::ObjectId(encoding) => Some(encoding),
            QuadStorageEncoding::PlainTerm => None,
        }
    }
}

#[allow(clippy::expect_used)]
fn object_id_quad_schema(encoding: &ObjectIdEncoding) -> DFSchemaRef {
    let data_type = encoding.data_type();
    Arc::new(
        DFSchema::from_unqualified_fields(
            Fields::from(vec![
                Field::new(COL_GRAPH, data_type.clone(), true),
                Field::new(COL_SUBJECT, data_type.clone(), false),
                Field::new(COL_PREDICATE, data_type.clone(), false),
                Field::new(COL_OBJECT, data_type, false),
            ]),
            HashMap::new(),
        )
        .expect("Fields are fixed"),
    )
}
