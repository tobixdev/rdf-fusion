use crate::plain_term::PlainTermEncoding;
use crate::sortable_term::SortableTermEncoding;
use crate::typed_value::TypedValueEncoding;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use rdf_fusion_common::DFResult;
use rdf_fusion_model::{TermRef, ThinResult};
use std::fmt::Debug;

/// Represents the name of a single [TermEncoding].
///
/// RdfFusion allows users to define multiple encodings for RDF terms. This allows specializing the
/// Arrow arrays used for holding the results of queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncodingName {
    /// Name of the [PlainTermEncoding]. Represents all terms, including literals, using their
    /// lexical value.
    PlainTerm,
    /// Name of the [TypedValueEncoding]. Represents IRIs and blank nodes using their lexical value
    /// and literals as their typed value.
    TypedValue,
    /// Name of the [SortableTermEncoding] which is used for sorting. We plan to remove this
    /// encoding in the future, once we can introduce custom orderings into the query engine.
    Sortable,
}

impl EncodingName {
    /// Tries to obtain an [EncodingName] from a [DataType]. As we currently only support built-in
    /// encodings this mapping is unique.
    ///
    /// It is planned to remove this function in the future for a state-full implementation that
    /// has access to registered custom encodings.
    pub fn try_from_data_type(data_type: &DataType) -> Option<Self> {
        if data_type == &TypedValueEncoding::data_type() {
            return Some(EncodingName::TypedValue);
        }

        if data_type == &PlainTermEncoding::data_type() {
            return Some(EncodingName::PlainTerm);
        }

        if data_type == &SortableTermEncoding::data_type() {
            return Some(EncodingName::Sortable);
        }

        None
    }
}

/// Represents an Arrow [Array] with a specific [TermEncoding].
///
/// The constructors of types that implement [EncodingArray] are meant to ensure that the
/// [ArrayRef] upholds all invariants of the encoding.
pub trait EncodingArray {
    /// Returns a reference to the inner array.
    fn array(&self) -> &ArrayRef;

    /// Consumes `self` and returns the inner array.
    fn into_array(self) -> ArrayRef;
}

/// Represents an Arrow [ScalarValue] with a specific [TermEncoding].
///
/// The constructors of types that implement [EncodingScalar] are meant to ensure that the
/// [ScalarValue] upholds all invariants of the encoding.
pub trait EncodingScalar {
    /// Returns a reference to the inner scalar value.
    fn scalar_value(&self) -> &ScalarValue;

    /// Consumes `self` and returns the inner scalar value.
    fn into_scalar_value(self) -> ScalarValue;
}

/// A term encoding defines how RDF terms are represented in Arrow arrays.
///
/// Each encoding defines a [DataType] that is uses for encoding RDF terms, while also having a
/// wrapper [Self::Array] and [Self::Scalar] for Arrow arrays and scalars.
///
/// Different term encodings usually have different purposes and may only be valid for certain
/// operations. For example, the [TypedValueEncoding] cannot be used to perform arbitrary join
/// operations as it does not retain the lexical value of the RDF literals. On the other hand,
/// the [TypedValueEncoding] will outperform the [PlainTermEncoding] for nested numerical
/// operations as the parsing and validation of numeric literals is only done once. It is up to the
/// user to ensure the correct use.
pub trait TermEncoding: Debug + Send + Sync {
    /// Represents a wrapper for Arrow arrays of this encoding. This can be used in
    /// conjunction with [TermDecoder] to obtain the values from an Arrow array.
    type Array: EncodingArray;
    /// Represents a wrapper for Arrow scalars of this encoding. This can be used in
    /// conjunction with [TermDecoder] to obtain the values from an Arrow scalar.
    type Scalar: EncodingScalar;

    /// Returns the name of the encoding.
    fn name() -> EncodingName;

    /// Returns the [DataType] that is used for this encoding.
    fn data_type() -> DataType;

    /// Checks whether `array` contains a value with the correct encoding (i.e., type and possibly
    /// metadata checks). If yes, returns an instance of [Self::Array]. Otherwise, an error is
    /// returned.
    fn try_new_array(array: ArrayRef) -> DFResult<Self::Array>;

    /// Checks whether `scalar` contains a value with the correct encoding (i.e., type and possibly
    /// metadata checks). If yes, returns an instance of [Self::Scalar]. Otherwise, an error is
    /// returned.
    fn try_new_scalar(scalar: ScalarValue) -> DFResult<Self::Scalar>;

    /// Checks whether `value` contains a value with the correct encoding (i.e., type and possibly
    /// metadata checks). If yes, returns a datum that either wraps an array or a scalar. Otherwise,
    /// an error is returned.
    fn try_new_datum(value: ColumnarValue, number_rows: usize) -> DFResult<EncodingDatum<Self>> {
        let datum = match value {
            ColumnarValue::Array(array) => {
                if array.len() != number_rows {
                    return exec_err!(
                        "Unexpected array size. Expected {number_rows}, Actual: {}",
                        array.len()
                    );
                }
                EncodingDatum::Array(Self::try_new_array(array)?)
            }
            ColumnarValue::Scalar(scalar) => {
                EncodingDatum::Scalar(Self::try_new_scalar(scalar)?, number_rows)
            }
        };
        Ok(datum)
    }

    /// Encodes the `term` in a type-safe scalar of this encoding.
    fn encode_scalar(term: TermRef<'_>) -> DFResult<Self::Scalar>;

    /// Encodes a null value in a type-safe scalar of this encoding.
    fn encode_null_scalar() -> DFResult<Self::Scalar>;
}

/// Allows extracting an iterator of a type from an [EncodingArray].
///
/// This allows uesrs to access the inner values of an RDF term array. It allows one to
/// obtain a typed iterator over the RDF terms in the array. A decoder is specialized for one
/// encoding and one value type ([Self::Term]).
///
/// ### Compatibility
///
/// Decoders are allowed to only support a subset of the encoded RDF terms. For example, a decoder
/// for boolean values may produce an error if it encounters a literal with a different type.
/// However, it is recommended that there is one decoder per [TermEncoding] that allows users to
/// extract all RDF terms.
///
/// ### Performance
///
/// Using a [TermDecoder] for accessing the array, performing an operation on [Self::Term], and then
/// re-encoding the resulting value using a [TermEncoder] may incur a performance penalty. However,
/// we hope that this impact can be mitigated by compiler optimizations. We have yet to benchmark
/// this impact to make a founded recommendation of when to use decoders and encoders. Users are
/// free to directly work on the Arrow arrays to side-step the typed Encoding/Decoding machinery.
pub trait TermDecoder<TEncoding: TermEncoding + ?Sized>: Debug + Sync + Send {
    /// The resulting value type of decoding an RDF term.
    type Term<'data>;

    /// Allows extracting an iterator over all RDF terms in `array` that are _compatible_ with this
    /// decoder (see [TermDecoder] for more information).
    ///
    /// The creation of the iterator cannot fail by itself, as the invariants of the encodings
    /// should have been checked while creating `array`. However, the iterator may return an error
    /// on every new value. This could be due to the value being incompatible with the decoder.
    fn decode_terms(array: &TEncoding::Array) -> impl Iterator<Item = ThinResult<Self::Term<'_>>>;

    /// Allows extracting an iterator over all RDF terms in `array` that are _compatible_ with this
    /// decoder (see [TermDecoder] for more information).
    ///
    /// The creation of the value can fail if the value stored in the `scalar` is incompatible with
    /// this decoder.
    fn decode_term(scalar: &TEncoding::Scalar) -> ThinResult<Self::Term<'_>>;
}

/// Allows encoding an iterator of a type into an [EncodingArray].
///
/// This allows users to encode values in an RDF term array. An encoder is specialized for
/// one encoding and one value type ([Self::Term]). The value type may only represent a subset of
/// all valid RDF terms (e.g., only Boolean values). However, it is recommended that there is
/// one decoder per [TermEncoding] that allows users to encode all RDF terms.
///
/// ### Performance
///
/// Using a [TermDecoder] for accessing the array, performing an operation on [Self::Term], and then
/// re-encoding the resulting value using a [TermEncoder] may incur a performance penalty. However,
/// we hope that this impact can be mitigated by compiler optimizations. We have yet to benchmark
/// this impact to make a founded recommendation of when to use decoders and encoders. Users are
/// free to directly work on the Arrow arrays to side-step the typed Encoding/Decoding machinery.
pub trait TermEncoder<TEncoding: TermEncoding + ?Sized>: Debug + Sync + Send {
    /// The value type that is being encoded.
    type Term<'data>;

    /// Allows encoding an iterator over RDF terms in an Arrow array.
    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<TEncoding::Array>;

    /// Allows encoding a scalar RDF term in an Arrow scalar.
    fn encode_term(term: ThinResult<Self::Term<'_>>) -> DFResult<TEncoding::Scalar> {
        let array = Self::encode_terms([term])?;
        let scalar = ScalarValue::try_from_array(array.array(), 0)?;
        TEncoding::try_new_scalar(scalar)
    }
}

/// Represents either an array or a scalar for a given encoding.
///
/// As the scalar variant also stores length information, one can obtain an iterator
/// ([Self::term_iter]) independently on whether the underlying data is an array or a scalar. This
/// is useful for scenarios in which distinguishing between array/scalar is not necessary or too
/// complex.
pub enum EncodingDatum<TEncoding: TermEncoding + ?Sized> {
    /// An array underlies this datum.
    Array(TEncoding::Array),
    /// A scalar underlies this datum. The additional length value is crucial for creating an
    /// iterator of a given length.
    Scalar(TEncoding::Scalar, usize),
}

impl<TEncoding: TermEncoding + ?Sized> EncodingDatum<TEncoding> {
    /// Creates an iterator over the contents of this datum.
    ///
    /// For an array, the iterator will simply return the result from the decoder.
    ///
    /// For a scalar, the value of the scalar will be cloned for each iteration, as dictated by the
    /// additional length.
    pub fn term_iter<'data, TDecoder>(
        &'data self,
    ) -> Box<dyn Iterator<Item = ThinResult<TDecoder::Term<'data>>> + 'data>
    where
        TDecoder: TermDecoder<TEncoding> + 'data,
    {
        match self {
            EncodingDatum::Array(array) => Box::new(
                TDecoder::decode_terms(array)
                    .collect::<Vec<_>>()
                    .into_iter(),
            ),
            EncodingDatum::Scalar(scalar, n) => {
                Box::new((0..*n).map(|_| TDecoder::decode_term(scalar)))
            }
        }
    }
}
