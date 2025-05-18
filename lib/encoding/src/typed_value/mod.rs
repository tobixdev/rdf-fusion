mod array;
mod builder;
pub mod decoders;
pub mod encoders;
mod encoding;
mod scalar;
mod scalar_encoder;

use crate::plain_term::PlainTermEncoding;
use crate::{TermEncoding, COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
pub use builder::TypedValueArrayBuilder;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use encoding::*;
use std::sync::LazyLock;

pub static DEFAULT_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    // TODO: be less lenient with nullability
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, PlainTermEncoding::data_type(), true),
        Field::new(COL_SUBJECT, PlainTermEncoding::data_type(), true),
        Field::new(COL_PREDICATE, PlainTermEncoding::data_type(), true),
        Field::new(COL_OBJECT, PlainTermEncoding::data_type(), true),
    ]))
});

pub static DEFAULT_QUAD_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| DFSchemaRef::new(DFSchema::try_from(DEFAULT_QUAD_SCHEMA.clone()).unwrap()));
