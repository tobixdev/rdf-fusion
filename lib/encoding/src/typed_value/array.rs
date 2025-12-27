use crate::encoding::EncodingArray;
use crate::typed_value::{TypedValueEncoding, TypedValueEncodingRef};
use crate::TermEncoding;
use datafusion::arrow::array::{
    Array, ArrayRef
    ,
};
use datafusion::common::exec_err;
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// Represents an Arrow array with a [`TypedValueEncoding`].
#[derive(Debug, Clone)]
pub struct TypedValueArray {
    /// The typed value encoding of this array.
    encoding: TypedValueEncodingRef,
    /// The Arrow array.
    inner: ArrayRef,
}

impl TypedValueArray {
    /// Tries to create a new [`TypedValueArray`] from the given `array` and `encoding`.
    ///
    /// Returns an error if the array does not match the given encoding.
    pub fn try_new(encoding: TypedValueEncodingRef, array: ArrayRef) -> DFResult<Self> {
        if array.data_type() != encoding.data_type() {
            return exec_err!(
                "Expected scalar value with TypedValueEncoding, got {:?}",
                array.data_type()
            );
        }

        Ok(Self::new_unchecked(encoding, array))
    }

    /// Creates a new [`TypedValueArray`] without verifying the schema.
    pub fn new_unchecked(encoding: TypedValueEncodingRef, array: ArrayRef) -> Self {
        Self {
            encoding,
            inner: array,
        }
    }
}

impl EncodingArray for TypedValueArray {
    type Encoding = TypedValueEncoding;

    fn encoding(&self) -> &Arc<Self::Encoding> {
        &self.encoding
    }

    fn array(&self) -> &ArrayRef {
        &self.inner
    }

    fn into_array_ref(self) -> ArrayRef {
        self.inner
    }
}
