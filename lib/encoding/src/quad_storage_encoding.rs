use crate::TermEncoding;
use crate::object_id::ObjectIdEncoding;
use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermEncoding};
use datafusion::arrow::datatypes::{DataType, Field, Fields, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::collections::HashMap;
use std::fmt::Display;
use std::sync::{Arc, LazyLock};

/// Defines which encoding is used for retrieving quads from the storage.
///
/// Defining this is necessary such that the query planner knows what type should be assigned to the
/// schema of quad pattern logical nodes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum QuadStorageEncoding {
    /// Uses the plain term encoding.
    ///
    /// Currently, the plain term encoding is not parameterizable. Therefore, this variant has no
    /// further information.
    PlainTerm,
    /// Uses the provided object id encoding.
    ObjectId(ObjectIdEncoding),
}

static PLAIN_TERM_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, PlainTermEncoding::data_type(), true),
        Field::new(COL_SUBJECT, PlainTermEncoding::data_type(), false),
        Field::new(COL_PREDICATE, PlainTermEncoding::data_type(), false),
        Field::new(COL_OBJECT, PlainTermEncoding::data_type(), false),
    ]))
});

static PLAIN_TERM_QUAD_DFSCHEMA: LazyLock<DFSchemaRef> = LazyLock::new(|| {
    DFSchemaRef::new(DFSchema::try_from(PLAIN_TERM_QUAD_SCHEMA.clone()).unwrap())
});

impl QuadStorageEncoding {
    /// Returns the data type of a single term column, given the current encoding.
    pub fn term_type(&self) -> DataType {
        match self {
            QuadStorageEncoding::PlainTerm => PLAIN_TERM_ENCODING.data_type(),
            QuadStorageEncoding::ObjectId(enc) => enc.data_type(),
        }
    }

    /// Returns the schema of an entire quad, given the current encoding.
    pub fn quad_schema(&self) -> DFSchemaRef {
        match self {
            QuadStorageEncoding::PlainTerm => PLAIN_TERM_QUAD_DFSCHEMA.clone(),
            QuadStorageEncoding::ObjectId(encoding) => object_id_quad_schema(encoding),
        }
    }

    /// Returns an optional reference to the contained [ObjectIdEncoding].
    ///
    /// Returns [None] otherwise.
    pub fn object_id_encoding(&self) -> Option<&ObjectIdEncoding> {
        match &self {
            QuadStorageEncoding::ObjectId(encoding) => Some(encoding),
            QuadStorageEncoding::PlainTerm => None,
        }
    }
}

impl Display for QuadStorageEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QuadStorageEncoding::PlainTerm => write!(f, "PlainTerm"),
            QuadStorageEncoding::ObjectId(encoding) => {
                write!(f, "ObjectId({} Bytes)", encoding.object_id_size())
            }
        }
    }
}

/// Computes the quad schema based on the given [ObjectIdEncoding].
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
