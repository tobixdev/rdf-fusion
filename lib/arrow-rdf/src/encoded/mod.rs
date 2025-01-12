mod cast;
mod model;
mod rdf_term_builder;
pub mod scalars;
mod udfs;

use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
pub use model::*;
use once_cell::unsync::Lazy;
pub use rdf_term_builder::RdfTermBuilder;
pub use udfs::register_rdf_term_udfs;

pub const ENC_QUAD_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, ENC_TYPE_TERM.clone(), false),
        Field::new(COL_SUBJECT, ENC_TYPE_TERM.clone(), false),
        Field::new(COL_PREDICATE, ENC_TYPE_TERM.clone(), false),
        Field::new(COL_OBJECT, ENC_TYPE_TERM.clone(), false),
    ]))
});
