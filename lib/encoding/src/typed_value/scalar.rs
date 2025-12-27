use crate::encoding::EncodingScalar;
use crate::typed_value::{TypedValueEncoding, TypedValueEncodingRef};
use crate::TermEncoding;
use datafusion::common::{exec_err, ScalarValue};
use rdf_fusion_model::{DFResult, ThinResult, TypedValueRef};
use std::sync::Arc;

/// Represents an Arrow scalar with a [`TypedValueEncoding`].
#[derive(Clone)]
pub struct TypedValueScalar {
    /// The [`TypedValueEncoding`] of this scalar.
    encoding: TypedValueEncodingRef,
    /// The actual [`ScalarValue`].
    inner: ScalarValue,
}

impl TypedValueScalar {
    /// Tries to create a new [`TypedValueScalar`] from a regular [`ScalarValue`].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(
        encoding: TypedValueEncodingRef,
        value: ScalarValue,
    ) -> DFResult<Self> {
        if &value.data_type() != encoding.data_type() {
            return exec_err!(
                "Expected scalar value with TypedValueEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(encoding, value))
    }

    /// Creates a new [`TypedValueScalar`] without checking invariants.
    pub fn new_unchecked(encoding: TypedValueEncodingRef, inner: ScalarValue) -> Self {
        Self { encoding, inner }
    }

    /// Returns a [`TypedValueRef`] to the underlying scalar.
    pub fn as_typed_value(&self) -> ThinResult<TypedValueRef<'_>> {
        todo!()
    }
}

impl EncodingScalar for TypedValueScalar {
    type Encoding = TypedValueEncoding;

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
