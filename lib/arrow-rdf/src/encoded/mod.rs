mod encoding;
mod model;
mod rdf_ops;
mod builder;
pub mod scalars;
#[macro_use]
mod macros;
mod dispatch;
mod from_encoded_term;
mod logical;
mod query_evaluation;
mod write_enc_term;

use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
pub use from_encoded_term::FromEncodedTerm;
pub use model::*;
use once_cell::unsync::Lazy;
pub use builder::EncRdfTermBuilder;

// Functions
pub use encoding::*;
pub use logical::*;
pub use query_evaluation::*;
pub use rdf_ops::*;

pub const ENC_QUAD_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, EncTerm::data_type(), false),
        Field::new(COL_SUBJECT, EncTerm::data_type(), false),
        Field::new(COL_PREDICATE, EncTerm::data_type(), false),
        Field::new(COL_OBJECT, EncTerm::data_type(), false),
    ]))
});
