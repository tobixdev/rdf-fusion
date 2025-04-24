mod kleene_plus;
mod path_node;

use arrow_rdf::encoded::EncTerm;
use arrow_rdf::COL_GRAPH;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use kleene_plus::*;
pub use path_node::*;
use std::clone::Clone;
use std::ops::Deref;
use std::sync::{Arc, LazyLock};

pub const COL_SOURCE: &str = "_source";
pub const COL_TARGET: &str = "_target";

pub static PATH_TABLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(COL_GRAPH, EncTerm::data_type(), true),
        Field::new(COL_SOURCE, EncTerm::data_type(), true),
        Field::new(COL_TARGET, EncTerm::data_type(), true),
    ]))
});

pub static PATH_TABLE_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| Arc::new(DFSchema::try_from(PATH_TABLE_SCHEMA.deref().clone()).unwrap()));
