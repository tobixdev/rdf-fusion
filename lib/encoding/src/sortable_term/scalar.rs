use crate::encoding::EncodingScalar;
use crate::sortable_term::SortableTermEncoding;
use crate::TermEncoding;
use datafusion::common::{exec_err, DataFusionError, ScalarValue};
use rdf_fusion_common::DFResult;
use crate::plain_term::PlainTermEncoding;

/// Represents an Arrow scalar with a [SortableTermEncoding].
pub struct SortableTermScalar {
    inner: ScalarValue,
}

impl SortableTermScalar {
    /// Tries to create a new [SortableTermScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(value: ScalarValue) -> DFResult<Self> {
        if value.data_type() != SortableTermEncoding::data_type() {
            return exec_err!(
                "Expected scalar value with SortableTermEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(value))
    }

    /// Creates a new [SortableTermScalar] without checking invariants.
    pub fn new_unchecked(inner: ScalarValue) -> Self {
        Self { inner }
    }
}

impl TryFrom<ScalarValue> for SortableTermScalar {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl EncodingScalar for SortableTermScalar {
    type Encoding = PlainTermEncoding;

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
