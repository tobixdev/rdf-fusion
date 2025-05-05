use crate::plain_term_encoding::PlainTermEncoding;
use crate::{DFResult, TermEncoding};
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use crate::encoding::EncodingScalar;

/// Represents an Arrow scalar with a [ValueEncoding].
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
        if value.data_type() != PlainTermEncoding::data_type() {
            return exec_err!("Expected scalar value with value encoding, got {:?}", value);
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
    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
