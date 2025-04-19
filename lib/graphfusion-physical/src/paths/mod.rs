mod kleene_plus;
mod path_node;

use arrow_rdf::encoded::EncTerm;
use arrow_rdf::COL_GRAPH;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::{DFSchema, DFSchemaRef};
use once_cell::sync::Lazy;
pub use path_node::*;
use std::sync::Arc;

const COL_SOURCE: &str = "_source";
const COL_TARGET: &str = "_target";
