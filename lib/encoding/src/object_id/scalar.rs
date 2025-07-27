use crate::TermEncoding;
use crate::encoding::EncodingScalar;
use crate::object_id::ObjectIdEncoding;
use datafusion::common::{ScalarValue, exec_err};
use rdf_fusion_common::DFResult;

/// Represents an Arrow scalar with a [ObjectIdEncoding].
pub struct ObjectIdScalar {
    encoding: ObjectIdEncoding,
    inner: ScalarValue,
}

impl ObjectIdScalar {
    /// Tries to create a new [ObjectIdScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(encoding: ObjectIdEncoding, value: ScalarValue) -> DFResult<Self> {
        if value.data_type() != encoding.data_type() {
            return exec_err!(
                "Expected scalar value with PlainTermEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(encoding, value))
    }

    /// Creates a new [ObjectIdScalar] without checking invariants.
    pub fn new_unchecked(encoding: ObjectIdEncoding, inner: ScalarValue) -> Self {
        Self { encoding, inner }
    }
}

impl EncodingScalar for ObjectIdScalar {
    type Encoding = ObjectIdEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &self.encoding
    }

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
