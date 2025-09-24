use crate::object_id::{ObjectIdArray, ObjectIdEncoding, ObjectIdScalar};
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use crate::typed_value::{TypedValueArray, TypedValueScalar};
use crate::{EncodingArray, EncodingScalar};
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use rdf_fusion_model::{CorruptionError, StorageError};
use std::fmt::Debug;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ObjectIdMappingError {
    #[error("An error occurred while encoding the result. {0}")]
    ArrowError(ArrowError),
    #[error("A literal was encountered at a position where a graph name is expected.")]
    LiteralAsGraphName,
    #[error("An unknown object ID was encountered in an unexpected place.")]
    UnknownObjectId,
}

#[derive(Error, Debug)]
#[error("An unknown object ID was encountered in an unexpected place.")]
pub struct UnknownObjectIdError;

impl From<ArrowError> for ObjectIdMappingError {
    fn from(value: ArrowError) -> Self {
        ObjectIdMappingError::ArrowError(value)
    }
}

impl From<ObjectIdMappingError> for DataFusionError {
    fn from(value: ObjectIdMappingError) -> Self {
        DataFusionError::External(Box::new(value))
    }
}

impl From<ObjectIdMappingError> for StorageError {
    fn from(value: ObjectIdMappingError) -> Self {
        StorageError::Corruption(CorruptionError::new(value))
    }
}

/// TODO
pub trait ObjectIdMapping: Debug + Send + Sync {
    /// Returns the encoding.
    fn encoding(&self) -> ObjectIdEncoding;

    /// TODO
    fn try_get_object_id(
        &self,
        scalar: &PlainTermScalar,
    ) -> Result<Option<ObjectIdScalar>, ObjectIdMappingError>;

    /// TODO
    fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<ObjectIdArray, ObjectIdMappingError>;

    /// TODO
    fn encode_scalar(
        &self,
        scalar: &PlainTermScalar,
    ) -> Result<ObjectIdScalar, ObjectIdMappingError> {
        let array = scalar
            .to_array(1)
            .expect("Data type is supported for to_array");
        let encoded = self.encode_array(&array)?;
        Ok(encoded.try_as_scalar(0).expect("Row 0 always exists"))
    }

    /// TODO
    fn decode_array(
        &self,
        array: &ObjectIdArray,
    ) -> Result<PlainTermArray, ObjectIdMappingError>;

    /// TODO
    fn decode_array_to_typed_value(
        &self,
        array: &ObjectIdArray,
    ) -> Result<TypedValueArray, ObjectIdMappingError>;

    /// TODO
    fn decode_scalar(
        &self,
        scalar: &ObjectIdScalar,
    ) -> Result<PlainTermScalar, ObjectIdMappingError> {
        let array = scalar
            .to_array(1)
            .expect("Data type is supported for to_array");
        let encoded = self.decode_array(&array)?;
        Ok(encoded.try_as_scalar(0).expect("Row 0 always exists"))
    }

    /// TODO
    fn decode_scalar_to_typed_value(
        &self,
        scalar: &ObjectIdScalar,
    ) -> Result<TypedValueScalar, ObjectIdMappingError> {
        let array = scalar
            .to_array(1)
            .expect("Data type is supported for to_array");
        let decoded = self.decode_array_to_typed_value(&array)?;
        Ok(decoded.try_as_scalar(0).expect("Row 0 always exists"))
    }
}
