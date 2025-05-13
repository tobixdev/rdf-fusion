use crate::encoding::EncodingArray;
use crate::plain_term::PlainTermEncoding;
use crate::TermEncoding;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;

/// Represents an Arrow array with a [PlainTermEncoding].
pub struct PlainTermArray {
    inner: ArrayRef,
}

impl PlainTermArray {}

impl EncodingArray for PlainTermArray {
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
        if value.data_type() != &PlainTermEncoding::data_type() {
            return exec_err!(
                "Expected array with PlainTermEncoding, got: {}",
                value.data_type()
            );
        }
        Ok(Self { inner: value })
    }
}
