mod array;
mod builder;
mod encoding;
mod extract;
mod from_arrow;
mod scalar;
mod scalar_encoder;
mod to_arrow;

use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
pub use builder::ValueArrayBuilder;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
pub use encoding::*;
use std::sync::LazyLock;

pub static ENC_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, TermValueEncoding::datatype(), true),
        Field::new(COL_SUBJECT, TermValueEncoding::datatype(), true),
        Field::new(COL_PREDICATE, TermValueEncoding::datatype(), true),
        Field::new(COL_OBJECT, TermValueEncoding::datatype(), true),
    ]))
});
