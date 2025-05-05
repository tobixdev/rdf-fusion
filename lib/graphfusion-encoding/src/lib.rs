use crate::plain_term_encoding::PlainTermEncoding;
use crate::value_encoding::TermValueEncoding;
use datafusion::arrow::array::{Array, UnionArray};
use datafusion::arrow::error::ArrowError;
use datafusion::common::{downcast_value, internal_err};
use datafusion::error::DataFusionError;

mod encoding;
pub mod error;
pub mod plain_term_encoding;
mod scalar_encoder;
pub mod sortable_encoding;
pub mod value_encoding;

pub use encoding::*;
pub use scalar_encoder::ScalarEncoder;

pub const TABLE_QUADS: &str = "quads";
pub const COL_GRAPH: &str = "graph";
pub const COL_SUBJECT: &str = "subject";
pub const COL_PREDICATE: &str = "predicate";
pub const COL_OBJECT: &str = "object";

type DFResult<T> = Result<T, DataFusionError>;
type AResult<T> = Result<T, ArrowError>;

/// Represents an instance of a GraphFusion encoding.
pub enum GraphFusionEncodedArray {
    /// Represents an Arrow array that contains entries with the [term_encoding].
    PlainTerm(PlainTermEncoding),
    /// Represents an Arrow array that contains entries with the [value_encoding].
    Value(TermValueEncoding),
    // Represents an Arrow array that contains entries with the [sortable_encoding].
    // Sortable(SortableEncodedArray),
}
