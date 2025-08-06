use crate::EncodingName;
use crate::encoding::TermEncoding;
use crate::object_id::{ObjectIdArray, ObjectIdScalar};
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use rdf_fusion_common::DFResult;
use std::clone::Clone;
use std::hash::Hash;

/// TODO
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ObjectIdEncoding {
    object_id_size: u8,
}

impl ObjectIdEncoding {
    /// Creates a new [ObjectIdEncoding].
    pub fn new(object_id_size: u8) -> Self {
        Self { object_id_size }
    }

    /// Returns the size of the object id.
    pub fn object_id_size(&self) -> u8 {
        self.object_id_size
    }

    /// Returns a null [ObjectIdScalar].
    pub fn null_scalar(&self) -> ObjectIdScalar {
        ObjectIdScalar::null(self.clone())
    }
}

impl TermEncoding for ObjectIdEncoding {
    type Array = ObjectIdArray;
    type Scalar = ObjectIdScalar;

    fn name(&self) -> EncodingName {
        EncodingName::PlainTerm
    }

    fn data_type(&self) -> DataType {
        DataType::UInt32
    }

    fn try_new_array(&self, array: ArrayRef) -> DFResult<Self::Array> {
        ObjectIdArray::try_new(self.clone(), array)
    }

    fn try_new_scalar(&self, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        ObjectIdScalar::try_new(self.clone(), scalar)
    }
}
