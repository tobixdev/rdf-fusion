use crate::plain_term::PlainTermEncoding;
use crate::typed_value::TypedValueEncoding;
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;

mod encoding;
pub mod plain_term;
mod scalar_encoder;
pub mod sortable_term;
pub mod typed_value;

pub use encoding::*;
pub use scalar_encoder::ScalarEncoder;

pub const TABLE_QUADS: &str = "quads";
pub const COL_GRAPH: &str = "graph";
pub const COL_SUBJECT: &str = "subject";
pub const COL_PREDICATE: &str = "predicate";
pub const COL_OBJECT: &str = "object";

type DFResult<T> = Result<T, DataFusionError>;
type AResult<T> = Result<T, ArrowError>;

/// Represents an instance of a RdfFusion encoding.
pub enum RdfFusionEncodedArray {
    /// Represents an Arrow array that contains entries with the [PlainTermEncoding].
    PlainTerm(PlainTermEncoding),
    /// Represents an Arrow array that contains entries with the [TypedValueEncoding].
    Value(TypedValueEncoding),
    // Represents an Arrow array that contains entries with the [sortable_encoding].
    // Sortable(SortableEncodedArray),
}
