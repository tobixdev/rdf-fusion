use crate::encoding::EncodingScalar;
use crate::typed_value::TypedValueEncoding;
use crate::TermEncoding;
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use rdf_fusion_common::DFResult;

/// Represents an Arrow scalar with a [TypedValueEncoding].
pub struct TypedValueScalar {
    /// The actual [ScalarValue].
    inner: ScalarValue,
}

impl TypedValueScalar {
    /// Tries to create a new [TypedValueScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(value: ScalarValue) -> DFResult<Self> {
        if value.data_type() != TypedValueEncoding::data_type() {
            return exec_err!(
                "Expected scalar value with TypedValueEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(value))
    }

    /// Creates a new [TypedValueScalar] without checking invariants.
    pub fn new_unchecked(inner: ScalarValue) -> Self {
        Self { inner }
    }
}

impl TryFrom<ScalarValue> for TypedValueScalar {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl EncodingScalar for TypedValueScalar {
    type Encoding = TypedValueEncoding;

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
