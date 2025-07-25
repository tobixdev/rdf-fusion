mod kleene_plus;
mod path_node;

use datafusion::arrow::datatypes::{Field, Fields, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use kleene_plus::*;
pub use path_node::*;
use rdf_fusion_encoding::QuadStorageEncoding;
use std::clone::Clone;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

pub const COL_PATH_GRAPH: &str = "_graph";
pub const COL_PATH_SOURCE: &str = "_source";
pub const COL_PATH_TARGET: &str = "_target";

/// Currently, property paths only work in the plain term encoding, as the physical Kleene node
/// operator only works for this encoding.
///
/// If the storage layer uses object ids, they are immediately decoded after loading.
pub static PATH_TABLE_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::clone(PATH_TABLE_DFSCHEMA.inner()));

pub static PATH_TABLE_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| create_path_table_schema(&QuadStorageEncoding::PlainTerm));

#[allow(clippy::expect_used)]
fn create_path_table_schema(storage_encoding: &QuadStorageEncoding) -> DFSchemaRef {
    let term_type = storage_encoding.term_type();
    Arc::new(
        DFSchema::from_unqualified_fields(
            Fields::from(vec![
                Field::new(COL_PATH_GRAPH, term_type.clone(), true),
                Field::new(COL_PATH_SOURCE, term_type.clone(), false),
                Field::new(COL_PATH_TARGET, term_type.clone(), false),
            ]),
            HashMap::new(),
        )
        .expect("Names fixed"),
    )
}
