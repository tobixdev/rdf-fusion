use crate::encoding::EncodingArray;
use crate::sortable_term::SortableTermEncoding;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;
use crate::TermEncoding;

/// Represents an Arrow array with a [SortableTermArray].
pub struct SortableTermArray {
    inner: ArrayRef,
}

impl SortableTermArray {}

impl EncodingArray for SortableTermArray {
    fn array(&self) -> &ArrayRef {
        &self.inner
    }

    fn into_array(self) -> ArrayRef {
        self.inner
    }
}

impl TryFrom<ArrayRef> for SortableTermArray {
    type Error = DataFusionError;

    fn try_from(value: ArrayRef) -> Result<Self, Self::Error> {
        if value.data_type() != &SortableTermEncoding::data_type() {
            return exec_err!(
                "Expected array with SortableEncoded terms, got: {}",
                value.data_type()
            );
        }
        Ok(Self { inner: value })
    }
}
