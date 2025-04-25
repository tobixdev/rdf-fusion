mod aggregates;
mod builder;
mod encoding;
mod model;
mod rdf_ops;
pub mod scalars;
#[macro_use]
mod macros;
mod dispatch;
mod from_encoded_term;
mod logical;
mod query_evaluation;
mod write_enc_term;

use crate::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
pub use builder::EncRdfTermBuilder;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
pub use from_encoded_term::FromEncodedTerm;
pub use model::*;
use std::sync::LazyLock;

// Functions
pub use aggregates::*;
pub use encoding::*;
pub use logical::*;
pub use query_evaluation::*;
pub use rdf_ops::*;

pub static ENC_QUAD_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    SchemaRef::new(Schema::new(vec![
        Field::new(COL_GRAPH, EncTerm::data_type(), true),
        Field::new(COL_SUBJECT, EncTerm::data_type(), true),
        Field::new(COL_PREDICATE, EncTerm::data_type(), true),
        Field::new(COL_OBJECT, EncTerm::data_type(), true),
    ]))
});
