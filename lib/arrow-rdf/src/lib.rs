use crate::encoded::EncTerm;
use datafusion::arrow::array::{Array, UnionArray};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{downcast_value, internal_err};
use datafusion::error::DataFusionError;
// TODO: Make DataFusion integration optional and use regular Arrow Crate

pub mod encoded;
pub mod error;
mod sortable;

pub const TABLE_QUADS: &str = "quads";
pub const COL_GRAPH: &str = "graph";
pub const COL_SUBJECT: &str = "subject";
pub const COL_PREDICATE: &str = "predicate";
pub const COL_OBJECT: &str = "object";

type DFResult<T> = Result<T, DataFusionError>;
type AResult<T> = Result<T, ArrowError>;

// Downcast ArrayRef to Int64Array
pub fn as_enc_term_array(array: &dyn Array) -> DFResult<&UnionArray> {
    if *array.data_type() != EncTerm::data_type() {
        return internal_err!("as_rdf_term_array expects a term type");
    }
    Ok(downcast_value!(array, UnionArray))
}
