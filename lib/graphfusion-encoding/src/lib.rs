use crate::plain_term_encoding::PlainTermEncoding;
use crate::value_encoding::TermValueEncoding;
use datafusion::arrow::array::{Array, ArrayRef, UnionArray};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use datafusion::common::{downcast_value, internal_err, ScalarValue};
use datafusion::error::DataFusionError;
use model::ThinResult;

pub mod error;
pub mod plain_term_encoding;
mod scalar_encoder;
pub mod sortable_encoding;
pub mod value_encoding;

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

/// TODO
pub trait TermEncoding {
    type Array: EncodingArray;
    type Scalar: EncodingScalar;

    /// Returns an implementation of a [ScalarEncoder] for this [TermEncoding]. The encoder can be
    /// used to encode RDF terms in scalars.
    type ScalarEncoder: ScalarEncoder<Scalar = Self::Scalar>;

    /// Checks whether `array` contains a value with the correct encoding (i.e., type and possibly
    /// metadata checks). If yes, returns an instance of the encoded array. Otherwise, an error is
    /// returned.
    fn try_new_array(array: ArrayRef) -> DFResult<Self::Array>;
}

/// Allows extracting an iterator of a type from an encoded array.
pub trait TermDecoder<'data, T>
where
    Self: EncodingArray,
{
    /// TODO
    fn decode_terms(&'data self) -> impl Iterator<Item = ThinResult<T>>;
}

/// Allows encoding an iterator of a type into an encoded array.
pub trait TermEncoder<'data, T>
where
    Self: EncodingArray,
{
    /// TODO
    fn encode_terms(terms: impl Iterator<Item = ThinResult<T>>) -> DFResult<Self>;
}

/// Represents an arrow [Array] with a specific Encoding.
///
/// The constructors of types that implement [EncodingArray] are meant to ensure that the
/// [ArrayRef] upholds all invariants of the encoding.
pub trait EncodingArray {
    /// Returns a reference to the inner array.
    fn array(&self) -> &ArrayRef;
}

/// Represents an arrow [ScalarValue] with a specific Encoding.
///
/// The constructors of types that implement [EncodingScalar] are meant to ensure that the
/// [ScalarValue] upholds all invariants of the encoding.
pub trait EncodingScalar {
    /// Returns a reference to the inner scalar value.
    fn scalar_value(&self) -> &ScalarValue;

    /// Consumes `self` and returns the inner scalar value.
    fn into_scalar_value(self) -> ScalarValue;
}

/// Allows encoding a value in an Arrow array.
pub trait ToArrow {
    /// Returns the datatype of the resulting [ScalarValue] or [ArrayRef] when calling
    /// [into_scalar_value] or [iter_into_array].
    fn encoded_datatype() -> DataType;

    /// Encodes a single value into a [ScalarValue].
    fn into_scalar_value(self) -> DFResult<ScalarValue>
    where
        Self: Sized,
    {
        let array = Self::iter_into_array([Ok(self)].into_iter())?;
        ScalarValue::try_from_array(&array, 0)
    }

    /// Encodes an iterator of values into an [ArrayRef].
    fn iter_into_array(values: impl Iterator<Item = ThinResult<Self>>) -> DFResult<ArrayRef>
    where
        Self: Sized;
}

/// Allows extracting a value from an Arrow array.
pub trait FromArrow<'data> {
    /// Extracts a value from an Arrow scalar.
    fn from_scalar(scalar: &'data ScalarValue) -> ThinResult<Self>
    where
        Self: Sized;

    /// Extracts a value from an Arrow array.
    fn from_array(array: &'data UnionArray, index: usize) -> ThinResult<Self>
    where
        Self: Sized;
}

pub fn as_term_value_array(array: &dyn Array) -> DFResult<&UnionArray> {
    if *array.data_type() != TermValueEncoding::datatype() {
        return internal_err!("as_rdf_term_array expects a term type");
    }
    Ok(downcast_value!(array, UnionArray))
}
