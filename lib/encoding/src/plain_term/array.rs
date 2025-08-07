use crate::TermEncoding;
use crate::encoding::EncodingArray;
use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermEncoding};
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;

/// Represents an Arrow array with a [PlainTermEncoding].
pub struct PlainTermArray {
    inner: ArrayRef,
}

impl PlainTermArray {
    /// Creates a new [PlainTermArray] without validating the schema.
    pub(super) fn new_unchecked(inner: ArrayRef) -> Self {
        Self { inner }
    }
}

impl EncodingArray for PlainTermArray {
    type Encoding = PlainTermEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &PLAIN_TERM_ENCODING
    }

    fn array(&self) -> &ArrayRef {
        &self.inner
    }

    fn into_array(self) -> ArrayRef {
        self.inner
    }
}

impl TryFrom<ArrayRef> for PlainTermArray {
    type Error = DataFusionError;

    fn try_from(value: ArrayRef) -> Result<Self, Self::Error> {
        if value.data_type() != &PLAIN_TERM_ENCODING.data_type() {
            return exec_err!(
                "Expected array with PlainTermEncoding, got: {}",
                value.data_type()
            );
        }
        Ok(Self { inner: value })
    }
}
