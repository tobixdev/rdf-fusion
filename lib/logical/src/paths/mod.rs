mod kleene_plus;
mod path_node;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use kleene_plus::*;
pub use path_node::*;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::TermEncoding;
use std::clone::Clone;
use std::sync::{Arc, LazyLock};

pub const COL_PATH_GRAPH: &str = "_graph";
pub const COL_PATH_SOURCE: &str = "_source";
pub const COL_PATH_TARGET: &str = "_target";

pub static PATH_TABLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(COL_PATH_GRAPH, PLAIN_TERM_ENCODING.data_type(), true),
        Field::new(COL_PATH_SOURCE, PLAIN_TERM_ENCODING.data_type(), false),
        Field::new(COL_PATH_TARGET, PLAIN_TERM_ENCODING.data_type(), false),
    ]))
});

pub static PATH_TABLE_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| Arc::new(DFSchema::try_from(Arc::clone(&*PATH_TABLE_SCHEMA)).unwrap()));
