use crate::encoding::EncodingScalar;
use crate::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermEncoding, PlainTermEncodingField, PlainTermType,
};
use datafusion::arrow::array::{Array, UInt8Array};
use datafusion::common::{DataFusionError, ScalarValue, exec_err};
use rdf_fusion_common::DFResult;

/// Represents an Arrow scalar with a [PlainTermEncoding].
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
            return exec_err!(
                "Expected scalar value with PlainTermEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(value))
    }

    /// Creates a new [PlainTermScalar] without checking invariants.
    pub fn new_unchecked(inner: ScalarValue) -> Self {
        Self { inner }
    }

    /// Returns the [PlainTermType] of this scalar.
    ///
    /// Returns [None] if this scalar is null.
    pub fn term_type(&self) -> Option<PlainTermType> {
        let ScalarValue::Struct(struct_array) = &self.inner else {
            unreachable!("PlainTermScalar must be a struct");
        };

        if struct_array.is_null(0) {
            return None;
        }

        let term_type = struct_array.column(PlainTermEncodingField::TermType.index());
        let value = term_type
            .as_any()
            .downcast_ref::<UInt8Array>()
            .expect("Expected StringArray")
            .value(0);
        Some(PlainTermType::try_from(value).expect("Expected valid term type"))
    }
}

impl TryFrom<ScalarValue> for PlainTermScalar {
    type Error = DataFusionError;

    fn try_from(value: ScalarValue) -> Result<Self, Self::Error> {
        Self::try_new(value)
    }
}

impl EncodingScalar for PlainTermScalar {
    type Encoding = PlainTermEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &PLAIN_TERM_ENCODING
    }

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
