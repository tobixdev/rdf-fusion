use crate::TermEncoding;
use crate::encoding::EncodingScalar;
use crate::object_id::ObjectIdEncoding;
use datafusion::common::{ScalarValue, exec_err};
use datafusion::parquet::data_type::AsBytes;
use rdf_fusion_common::{DFResult, ObjectIdRef};

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
                "Expected scalar value with ObjectID encoding. Expected: {:?}, got {:?}",
                encoding.data_type(),
                value.data_type()
            );
        }
        Ok(Self::new_unchecked(encoding, value))
    }

    /// Creates a new [ObjectIdScalar] without checking invariants.
    pub fn new_unchecked(encoding: ObjectIdEncoding, inner: ScalarValue) -> Self {
        Self { encoding, inner }
    }

    /// Creates a new [ObjectIdScalar] from the given `object_id`.
    pub fn null(encoding: ObjectIdEncoding) -> Self {
        let scalar = ScalarValue::FixedSizeBinary(encoding.object_id_size() as i32, None);
        Self::new_unchecked(encoding, scalar)
    }

    /// Creates a new [ObjectIdScalar] from the given `object_id`.
    pub fn from_object_id(
        encoding: ObjectIdEncoding,
        object_id: ObjectIdRef<'_>,
    ) -> Self {
        let scalar = ScalarValue::FixedSizeBinary(
            encoding.object_id_size() as i32,
            Some(object_id.as_bytes().to_vec()),
        );
        Self::new_unchecked(encoding, scalar)
    }

    /// Returns an [ObjectId] from this scalar.
    pub fn as_object_ref(&self) -> Option<ObjectIdRef> {
        match &self.inner {
            ScalarValue::FixedSizeBinary(_, bytes) => {
                bytes.as_ref().map(|b| ObjectIdRef::from(b.as_bytes()))
            }
            _ => unreachable!("Checked in constructor."),
        }
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
