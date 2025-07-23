use crate::encoding::EncodingArray;
use crate::object_id::ObjectIdEncoding;
use crate::TermEncoding;
use datafusion::arrow::array::{Array, ArrayRef, UInt64Array};
use datafusion::common::exec_err;
use rdf_fusion_common::DFResult;

/// Represents an Arrow array with a [Objec].
pub struct ObjectIdArray {
    encoding: ObjectIdEncoding,
    inner: ArrayRef,
}

impl ObjectIdArray {
    /// Tries to create a new [ObjectIdScalar] from a regular [ScalarValue].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(encoding: ObjectIdEncoding, array: ArrayRef) -> DFResult<Self> {
        if array.data_type() != &encoding.data_type() {
            return exec_err!("Expected array with ObjectIdEncoding, got {:?}", array);
        }
        Ok(Self::new_unchecked(encoding, array))
    }

    /// Creates a new [ObjectIdScalar] without checking invariants.
    pub fn new_unchecked(encoding: ObjectIdEncoding, inner: ArrayRef) -> Self {
        Self { encoding, inner }
    }

    /// Returns a reference to the inner [UInt64Array].
    pub fn object_ids(&self) -> &UInt64Array {
        self.inner
            .as_any()
            .downcast_ref::<UInt64Array>()
            .expect("Checked in constructor")
    }
}

impl EncodingArray for ObjectIdArray {
    type Encoding = ObjectIdEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &self.encoding
    }

    fn array(&self) -> &ArrayRef {
        &self.inner
    }

    fn into_array(self) -> ArrayRef {
        self.inner
    }
}
