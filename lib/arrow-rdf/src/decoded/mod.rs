use crate::decoded::model::DecTerm;
use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use once_cell::unsync::Lazy;

pub mod model;
mod rdf_term_builder;

pub use rdf_term_builder::DecRdfTermBuilder;

pub const DEC_QUAD_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, DecTerm::term_type(), false),
        Field::new(COL_SUBJECT, DecTerm::term_type(), false),
        Field::new(COL_PREDICATE, DecTerm::term_type(), false),
        Field::new(COL_OBJECT, DecTerm::term_type(), false),
    ]))
});
