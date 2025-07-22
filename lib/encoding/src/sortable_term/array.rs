use crate::encoding::EncodingArray;
use crate::sortable_term::{SortableTermEncoding, SORTABLE_TERM_ENCODING};
use crate::TermEncoding;
use datafusion::arrow::array::{Array, ArrayRef};
use datafusion::common::exec_err;
use datafusion::error::DataFusionError;

/// Represents an Arrow array with a [SortableTermArray].
pub struct SortableTermArray {
    inner: ArrayRef,
}

impl SortableTermArray {}

impl EncodingArray for SortableTermArray {
    type Encoding = SortableTermEncoding;

    fn encoding(&self) -> &Self::Encoding {
        &SORTABLE_TERM_ENCODING
    }

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
        if value.data_type() != &SORTABLE_TERM_ENCODING.data_type() {
            return exec_err!(
                "Expected array with SortableEncoded terms, got: {}",
                value.data_type()
            );
        }
        Ok(Self { inner: value })
    }
}
