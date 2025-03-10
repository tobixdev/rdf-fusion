mod model;
mod rdf_term_builder;
pub mod scalars;
mod udfs;
mod strings;
mod terms;
mod dispatch_binary;
mod dispatch_unary;
mod dispatch;
mod functional_forms;
mod dispatch_ternary;
mod query_evaluation;
mod encoding;
mod arithmetic;

use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
pub use model::*;
use once_cell::unsync::Lazy;
pub use rdf_term_builder::EncRdfTermBuilder;

// Functions
pub use arithmetic::*;
pub use encoding::*;
pub use functional_forms::*;
pub use query_evaluation::*;
pub use terms::*;
pub use strings::*;
pub use udfs::*;

pub const ENC_QUAD_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, EncTerm::term_type(), false),
        Field::new(COL_SUBJECT, EncTerm::term_type(), false),
        Field::new(COL_PREDICATE, EncTerm::term_type(), false),
        Field::new(COL_OBJECT, EncTerm::term_type(), false),
    ]))
});
