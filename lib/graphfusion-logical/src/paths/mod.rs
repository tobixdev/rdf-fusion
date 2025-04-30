mod kleene_plus;
mod path_node;

use arrow_rdf::value_encoding::RdfValueEncoding;
use arrow_rdf::COL_GRAPH;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use kleene_plus::*;
pub use path_node::*;
use std::clone::Clone;
use std::sync::{Arc, LazyLock};

pub const COL_SOURCE: &str = "_source";
pub const COL_TARGET: &str = "_target";

pub static PATH_TABLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(COL_GRAPH, RdfValueEncoding::data_type(), true),
        Field::new(COL_SOURCE, RdfValueEncoding::data_type(), true),
        Field::new(COL_TARGET, RdfValueEncoding::data_type(), true),
    ]))
});

pub static PATH_TABLE_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| Arc::new(DFSchema::try_from(Arc::clone(&*PATH_TABLE_SCHEMA)).unwrap()));
