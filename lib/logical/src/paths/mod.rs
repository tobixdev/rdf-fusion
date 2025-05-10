mod kleene_plus;
mod path_node;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
use graphfusion_encoding::plain_term_encoding::PlainTermEncoding;
pub use kleene_plus::*;
pub use path_node::*;
use std::clone::Clone;
use std::sync::{Arc, LazyLock};
use graphfusion_encoding::TermEncoding;

pub const COL_GRAPH: &str = "_graph";
pub const COL_SOURCE: &str = "_source";
pub const COL_TARGET: &str = "_target";

pub static PATH_TABLE_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new(COL_GRAPH, PlainTermEncoding::data_type(), true),
        Field::new(COL_SOURCE, PlainTermEncoding::data_type(), false),
        Field::new(COL_TARGET, PlainTermEncoding::data_type(), false),
    ]))
});

pub static PATH_TABLE_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| Arc::new(DFSchema::try_from(Arc::clone(&*PATH_TABLE_SCHEMA)).unwrap()));
