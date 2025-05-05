use crate::{DFResult, ScalarEncoder};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::logical_expr::ColumnarValue;
use model::ThinResult;

/// TODO
pub type DefaultDecoder<TEncoding> = <TEncoding as TermEncoding>::DefaultDecoder;
pub type DefaultDecoderTerm<'data, TEncoding> =
    <DefaultDecoder<TEncoding> as TermDecoder<TEncoding>>::Term<'data>;

/// TODO
pub type DefaultEncoder<TEncoding> = <TEncoding as TermEncoding>::DefaultEncoder;
pub type DefaultEncoderTerm<'data, TEncoding> =
    <DefaultEncoder<TEncoding> as TermEncoder<TEncoding>>::Term<'data>;

/// Represents an arrow [Array] with a specific Encoding.
///
/// The constructors of types that implement [EncodingArray] are meant to ensure that the
/// [ArrayRef] upholds all invariants of the encoding.
pub trait EncodingArray {
    /// Returns a reference to the inner array.
    fn array(&self) -> &ArrayRef;

    /// Consumes `self` and returns the inner array.
    fn into_array(self) -> ArrayRef;
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

/// TODO
pub trait TermEncoding {
    type Array: EncodingArray;
    type Scalar: EncodingScalar;
    type DefaultEncoder: TermEncoder<Self>;
    type DefaultDecoder: TermDecoder<Self>;

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
}

/// Allows extracting an iterator of a type from an [EncodingArray].
pub trait TermDecoder<TEncoding: TermEncoding + ?Sized> {
    type Term<'data>;

    /// TODO
    fn decode_terms(array: &TEncoding::Array) -> impl Iterator<Item = ThinResult<Self::Term<'_>>>;

    /// TODO
    fn decode_term(scalar: &TEncoding::Scalar) -> ThinResult<Self::Term<'_>>;
}

/// Allows encoding an iterator of a type into an [EncodingArray].
pub trait TermEncoder<TEncoding: TermEncoding + ?Sized> {
    type Term<'data>;

    /// TODO
    fn encode_terms<'data>(
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<TEncoding::Array>;

    /// TODO
    fn encode_term(term: ThinResult<Self::Term<'_>>) -> DFResult<TEncoding::Scalar>;
}

/// TODO
pub enum EncodingDatum<TEncoding: TermEncoding + ?Sized> {
    /// TODO
    Array(TEncoding::Array),
    /// TODO
    Scalar(TEncoding::Scalar, usize),
}

/// TODO
pub struct DatumIterator<'a, TEncoding: TermEncoding + ?Sized, D: TermDecoder<TEncoding>> {
    state: DatumIteratorState<'a, TEncoding, D>,
}

/// TODO
enum DatumIteratorState<'a, TEncoding: TermEncoding + ?Sized, D: TermDecoder<TEncoding>> {
    Array {
        iter: Box<dyn Iterator<Item = ThinResult<D::Term<'a>>> + 'a>,
    },
    Scalar {
        value: &'a TEncoding::Scalar,
        remaining: usize,
        _decoder: std::marker::PhantomData<D>,
    },
}

impl<TEncoding: TermEncoding> EncodingDatum<TEncoding> {
    /// Returns an iterator over the values in this datum.
    pub fn iter<'a, D>(&'a self) -> DatumIterator<'a, TEncoding, D>
    where
        D: TermDecoder<TEncoding> + 'a,
    {
        let state = match self {
            EncodingDatum::Array(array) => DatumIteratorState::Array {
                iter: Box::new(D::decode_terms(array)),
            },
            EncodingDatum::Scalar(scalar, len) => DatumIteratorState::Scalar {
                value: scalar,
                remaining: *len,
                _decoder: std::marker::PhantomData,
            },
        };
        DatumIterator { state }
    }
}

impl<'a, TEncoding: TermEncoding, D: TermDecoder<TEncoding>> Iterator
    for DatumIterator<'a, TEncoding, D>
{
    type Item = ThinResult<D::Term<'a>>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            DatumIteratorState::Array { iter } => iter.next(),
            DatumIteratorState::Scalar {
                value, remaining, ..
            } => {
                if *remaining == 0 {
                    None
                } else {
                    *remaining -= 1;
                    Some(D::decode_term(value))
                }
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        match &self.state {
            DatumIteratorState::Array { iter } => iter.size_hint(),
            DatumIteratorState::Scalar { remaining, .. } => (*remaining, Some(*remaining)),
        }
    }
}

impl<'a, TEncoding: TermEncoding, D: TermDecoder<TEncoding>> ExactSizeIterator
    for DatumIterator<'a, TEncoding, D>
{
}
