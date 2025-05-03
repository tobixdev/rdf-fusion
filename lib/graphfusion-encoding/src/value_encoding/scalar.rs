use crate::value_encoding::TermValueEncoding;
use crate::{DFResult, EncodingScalar};
use datafusion::common::{exec_err, DataFusionError, ScalarValue};

/// Represents an Arrow scalar with a [TermValueEncoding].
pub struct TermValueScalar {
    inner: ScalarValue,
}

impl TermValueScalar {
    /// Tries to create a new [TermValueScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(value: ScalarValue) -> DFResult<Self> {
        if value.data_type() != TermValueEncoding::datatype() {
            return exec_err!("Expected scalar value with value encoding, got {:?}", value);
        }
        Ok(Self::new_unchecked(value))
    }

    /// Creates a new [TermValueScalar] without checking invariants.
    pub fn new_unchecked(inner: ScalarValue) -> Self {
        Self { inner }
    }
}

impl TryFrom<ScalarValue> for TermValueScalar {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl EncodingScalar for TermValueScalar {
    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
