use crate::decoded::model::DEC_TYPE_TERM;
use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use once_cell::unsync::Lazy;

pub mod model;

pub const DEC_QUAD_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, DEC_TYPE_TERM.clone(), false),
        Field::new(COL_SUBJECT, DEC_TYPE_TERM.clone(), false),
        Field::new(COL_PREDICATE, DEC_TYPE_TERM.clone(), false),
        Field::new(COL_OBJECT, DEC_TYPE_TERM.clone(), false),
    ]))
});
