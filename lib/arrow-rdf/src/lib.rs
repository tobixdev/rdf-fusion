use datafusion::arrow::array::{Array, UnionArray};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{downcast_value, internal_err};
use datafusion::error::DataFusionError;
use crate::encoded::EncTerm;
// TODO: Make DataFusion integration optional and use regular Arrow Crate

pub mod decoded;
pub mod encoded;
mod error;
mod result_collector;
mod sorting;

pub const RDF_DECIMAL_PRECISION: u8 = 36;
pub const RDF_DECIMAL_SCALE: i8 = 18;

pub const TABLE_QUADS: &str = "quads";
pub const COL_GRAPH: &str = "graph";
pub const COL_SUBJECT: &str = "subject";
pub const COL_PREDICATE: &str = "predicate";
pub const COL_OBJECT: &str = "object";

type DFResult<T> = Result<T, DataFusionError>;
type AResult<T> = Result<T, ArrowError>;

// Downcast ArrayRef to Int64Array
pub fn as_enc_term_array(array: &dyn Array) -> DFResult<&UnionArray> {
    if *array.data_type() != EncTerm::term_type() {
        return internal_err!("as_rdf_term_array expects a term type");
    }
    Ok(downcast_value!(array, UnionArray))
}
