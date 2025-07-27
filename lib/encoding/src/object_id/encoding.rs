use crate::encoding::TermEncoding;
use crate::object_id::mapping::ObjectIdMapping;
use crate::object_id::{ObjectIdArray, ObjectIdScalar};
use crate::EncodingName;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use rdf_fusion_common::DFResult;
use rdf_fusion_model::{TermRef, ThinResult};
use std::clone::Clone;
use std::hash::{Hash, Hasher};
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

    /// Returns the data type of the [ObjectIdEncoding].
    ///
    /// The type of the [ObjectIdEncoding] is statically known and cannot be configured.
    pub fn data_type() -> DataType {
        DataType::UInt64
    }
}

impl TermEncoding for ObjectIdEncoding {
    type Array = ObjectIdArray;
    type Scalar = ObjectIdScalar;

    fn name(&self) -> EncodingName {
        EncodingName::PlainTerm
    }

    fn data_type(&self) -> DataType {
        ObjectIdEncoding::data_type()
    }

    fn try_new_array(&self, array: ArrayRef) -> DFResult<Self::Array> {
        ObjectIdArray::try_new(self.clone(), array)
    }

    fn try_new_scalar(&self, scalar: ScalarValue) -> DFResult<Self::Scalar> {
        ObjectIdScalar::try_new(self.clone(), scalar)
    }

    fn encode_term(&self, term: ThinResult<TermRef<'_>>) -> DFResult<Self::Scalar> {
        match term {
            Ok(term) => {
                let encoded = self.mapping.encode(term);
                self.try_new_scalar(ScalarValue::UInt64(Some(encoded)))
            }
            Err(_) => self.try_new_scalar(ScalarValue::UInt64(None)),
        }
    }
}

impl PartialEq for ObjectIdEncoding {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.mapping, &other.mapping)
    }
}

impl Eq for ObjectIdEncoding {}

impl Hash for ObjectIdEncoding {
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.mapping).hash(state);
    }
}
