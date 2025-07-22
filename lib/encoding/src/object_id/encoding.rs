use crate::encoding::TermEncoding;
use crate::object_id::mapping::ObjectIdMapping;
use crate::object_id::{ObjectIdArray, ObjectIdScalar};
use crate::EncodingName;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{plan_err, ScalarValue};
use rdf_fusion_common::DFResult;
use rdf_fusion_model::{TermRef, ThinResult};
use std::clone::Clone;
use std::sync::Arc;

/// TODO
#[derive(Debug, Clone)]
pub struct ObjectIdEncoding {
    mapping: Arc<dyn ObjectIdMapping>,
}

impl ObjectIdEncoding {
    /// Creates a new [ObjectIdEncoding].
    pub fn new(mapping: Arc<dyn ObjectIdMapping>) -> Self {
        Self { mapping }
    }

    /// Returns a reference to the object id mapping.
    pub fn mapping(&self) -> &dyn ObjectIdMapping {
        self.mapping.as_ref()
    }
}

impl TermEncoding for ObjectIdEncoding {
    type Array = ObjectIdArray;
    type Scalar = ObjectIdScalar;

    fn name(&self) -> EncodingName {
        EncodingName::PlainTerm
    }

    fn data_type(&self) -> DataType {
        DataType::Int64
    }

    fn try_new_array(&self, array: ArrayRef) -> DFResult<Self::Array> {
        ObjectIdArray::try_new(self.clone(), array)
    }

    fn try_new_scalar(&self, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        ObjectIdScalar::try_new(self.clone(), scalar)
    }

    fn encode_term(&self, _term: ThinResult<TermRef<'_>>) -> DFResult<Self::Scalar> {
        plan_err!("Currently not supported")
    }
}
