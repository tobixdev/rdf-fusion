use crate::TermEncoding;
use crate::encoding::EncodingScalar;
use crate::object_id::ObjectIdEncoding;
use datafusion::common::{ScalarValue, exec_err};
use rdf_fusion_model::{DFResult, ObjectId};
use std::sync::Arc;

/// Represents an Arrow scalar with a [ObjectIdEncoding].
pub struct ObjectIdScalar {
    encoding: Arc<ObjectIdEncoding>,
    inner: ScalarValue,
}

impl ObjectIdScalar {
    /// Tries to create a new [ObjectIdScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(
        encoding: Arc<ObjectIdEncoding>,
        value: ScalarValue,
    ) -> DFResult<Self> {
        if &value.data_type() != encoding.data_type() {
            return exec_err!(
                "Expected scalar value with ObjectID encoding. Expected: {:?}, got {:?}",
                encoding.data_type(),
                value.data_type()
            );
        }
        Ok(Self::new_unchecked(encoding, value))
    }

    /// Creates a new [ObjectIdScalar] without checking invariants.
    pub fn new_unchecked(encoding: Arc<ObjectIdEncoding>, inner: ScalarValue) -> Self {
        Self { encoding, inner }
    }

    /// Creates a new [ObjectIdScalar] from the given `object_id`.
    pub fn null(encoding: Arc<ObjectIdEncoding>) -> Self {
        let scalar = ScalarValue::UInt32(None);
        Self::new_unchecked(encoding, scalar)
    }

    /// Creates a new [ObjectIdScalar] from the given `object_id`.
    pub fn from_object_id(encoding: Arc<ObjectIdEncoding>, object_id: ObjectId) -> Self {
        let scalar = ScalarValue::UInt32(Some(object_id.0));
        Self::new_unchecked(encoding, scalar)
    }

    /// Returns an [ObjectId] from this scalar.
    pub fn as_object(&self) -> Option<ObjectId> {
        match &self.inner {
            ScalarValue::UInt32(scalar) => scalar.map(ObjectId::from),
            _ => unreachable!("Checked in constructor."),
        }
    }
}

impl EncodingScalar for ObjectIdScalar {
    type Encoding = ObjectIdEncoding;

    fn encoding(&self) -> &Arc<Self::Encoding> {
        &self.encoding
    }

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
