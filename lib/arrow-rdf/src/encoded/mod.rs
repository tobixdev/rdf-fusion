mod comparison;
mod conversion;
mod dispatch_binary;
mod dispatch_ternary;
mod dispatch_unary;
mod encoding;
mod functional_forms;
mod model;
mod numeric;
mod query_evaluation;
mod rdf_term_builder;
pub mod scalars;
mod strings;
mod terms;
mod udfs;
mod dispatch_quaternary;
mod logical;

use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
pub use model::*;
use once_cell::unsync::Lazy;
pub use rdf_term_builder::EncRdfTermBuilder;

// Functions
pub use comparison::*;
pub use conversion::*;
pub use encoding::*;
pub use functional_forms::*;
pub use logical::*;
pub use numeric::*;
pub use query_evaluation::*;
pub use strings::*;
pub use terms::*;
pub use udfs::*;

pub const ENC_QUAD_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, EncTerm::term_type(), false),
        Field::new(COL_SUBJECT, EncTerm::term_type(), false),
        Field::new(COL_PREDICATE, EncTerm::term_type(), false),
        Field::new(COL_OBJECT, EncTerm::term_type(), false),
    ]))
});
