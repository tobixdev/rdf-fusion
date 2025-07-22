mod array;
mod builder;
pub mod decoders;
pub mod encoders;
mod encoding;
mod scalar;

use crate::plain_term::PLAIN_TERM_ENCODING;
use crate::{TermEncoding, COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
pub use array::{TypedValueArray, TypedValueArrayParts};
pub use builder::TypedValueArrayBuilder;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use encoding::*;
pub use scalar::TypedValueScalar;
use std::sync::LazyLock;

pub static PLAIN_TERM_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, PLAIN_TERM_ENCODING.data_type(), true),
        Field::new(COL_SUBJECT, PLAIN_TERM_ENCODING.data_type(), false),
        Field::new(COL_PREDICATE, PLAIN_TERM_ENCODING.data_type(), false),
        Field::new(COL_OBJECT, PLAIN_TERM_ENCODING.data_type(), false),
    ]))
});

pub static PLAIN_TERM_QUAD_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| DFSchemaRef::new(DFSchema::try_from(PLAIN_TERM_QUAD_SCHEMA.clone()).unwrap()));
