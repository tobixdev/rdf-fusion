mod array;
mod builder;
pub mod decoders;
pub mod encoders;
mod encoding;
mod scalar;
mod scalar_encoder;

use crate::{TermEncoding, COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
pub use builder::TermValueArrayBuilder;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
pub use encoding::*;
use std::sync::LazyLock;

pub static ENC_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, TermValueEncoding::data_type(), true),
        Field::new(COL_SUBJECT, TermValueEncoding::data_type(), true),
        Field::new(COL_PREDICATE, TermValueEncoding::data_type(), true),
        Field::new(COL_OBJECT, TermValueEncoding::data_type(), true),
    ]))
});
