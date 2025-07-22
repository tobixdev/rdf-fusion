use crate::encoding::EncodingScalar;
use crate::plain_term::{PlainTermEncoding, PLAIN_TERM_ENCODING};
use crate::TermEncoding;
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use rdf_fusion_common::DFResult;

/// Represents an Arrow scalar with a [PlainTermEncoding].
pub struct PlainTermScalar {
    inner: ScalarValue,
}

impl PlainTermScalar {
    /// Tries to create a new [PlainTermScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(value: ScalarValue) -> DFResult<Self> {
        if value.data_type() != PLAIN_TERM_ENCODING.data_type() {
            return exec_err!(
                "Expected scalar value with PlainTermEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(value))
    }

    /// Creates a new [PlainTermScalar] without checking invariants.
    pub fn new_unchecked(inner: ScalarValue) -> Self {
        Self { inner }
    }
}

impl TryFrom<ScalarValue> for PlainTermScalar {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl EncodingScalar for PlainTermScalar {
    type Encoding = PlainTermEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &PLAIN_TERM_ENCODING
    }

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
