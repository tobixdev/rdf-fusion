use crate::AResult;
use datafusion::arrow::array::ArrayRef;
use datafusion::common::ScalarValue;
use datamodel::RdfOpResult;

pub trait WriteEncTerm {
    fn into_scalar_value(self) -> AResult<ScalarValue>
    where
        Self: Sized;

    fn iter_into_array(values: impl Iterator<Item = RdfOpResult<Self>>) -> AResult<ArrayRef>
    where
        Self: Sized;
}
