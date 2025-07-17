mod array;
mod builder;
pub mod decoders;
pub mod encoders;
mod encoding;
mod scalar;

use crate::plain_term::PlainTermEncoding;
use crate::{TermEncoding, COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
pub use array::TypedValueArray;
pub use builder::TypedValueArrayBuilder;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use encoding::*;
pub use scalar::TypedValueScalar;
use std::sync::LazyLock;

pub static DEFAULT_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, PlainTermEncoding::data_type(), true),
        Field::new(COL_SUBJECT, PlainTermEncoding::data_type(), false),
        Field::new(COL_PREDICATE, PlainTermEncoding::data_type(), false),
        Field::new(COL_OBJECT, PlainTermEncoding::data_type(), false),
    ]))
});

pub static DEFAULT_QUAD_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| DFSchemaRef::new(DFSchema::try_from(DEFAULT_QUAD_SCHEMA.clone()).unwrap()));
