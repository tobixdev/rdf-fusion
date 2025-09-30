use crate::object_id::{ObjectIdArray, ObjectIdEncoding, ObjectIdScalar};
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use crate::typed_value::{TypedValueArray, TypedValueScalar};
use crate::{EncodingArray, EncodingScalar};
use datafusion::arrow::error::ArrowError;
use datafusion::error::DataFusionError;
use rdf_fusion_model::{CorruptionError, StorageError};
use std::error::Error;
use std::fmt::Debug;
use thiserror::Error;

/// Indicates an error that occurred while working with the [ObjectIdMapping].
#[derive(Error, Debug)]
pub enum ObjectIdMappingError {
    #[error("An error occurred while encoding the result. {0}")]
    ArrowError(ArrowError),
    #[error("A literal was encountered at a position where a graph name is expected.")]
    LiteralAsGraphName,
    #[error("An unknown object ID was encountered in an unexpected place.")]
    UnknownObjectId,
    #[error("An error occurred while accessing the object id storage.")]
    Storage(Box<dyn Error + Sync + Send>),
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

/// The object id mapping is responsible for mapping between object ids and RDF terms in the
/// [ObjectIdEncoding].
///
/// The mapping between the object id and the RDF term is bijective. In other words, each distinct
/// RDF term maps to exactly one object id, while each object id maps to exactly one RDF term. As
/// a result, operations that rely on the equality of RDF terms (`SAME_TERM`) can directly work
/// with the object ids. Joining solution sets is the most important example.
///
/// # Typed Values
///
/// To speed up decoding object ids directly into the [TypedValueEncoding](crate::typed_value::TypedValueEncoding),
/// the trait also contains methods for directly mapping object ids to their typed values. This can
/// be implemented in two ways:
/// 1. Decode the object id to a plain term and then translate the term to a typed value
/// 2. Maintain a second mapping from the object ids to the typed value of their associated RDF term
///
/// Contrary to the mapping between RDF terms and object ids, the mapping between typed values and
/// object ids is not bijective. A single typed value can map to multiple object ids. For example,
/// this is the case for the two RDF terms `"01"^^xsd:integer` and `"1"^^xsd:integer`.
pub trait ObjectIdMapping: Debug + Send + Sync {
    /// Returns the encoding.
    fn encoding(&self) -> ObjectIdEncoding;

    /// Try to retrieve the object id of the given `scalar`.
    ///
    /// This method *does not* automatically create a mapping. See [Self::encode_scalar] for this
    /// functionality.
    fn try_get_object_id(
        &self,
        scalar: &PlainTermScalar,
    ) -> Result<Option<ObjectIdScalar>, ObjectIdMappingError>;

    /// Encodings the entire `array` as an [ObjectIdArray]. Automatically creates a mapping for a
    /// fresh object id if a term is not yet mapped.
    fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<ObjectIdArray, ObjectIdMappingError>;

    /// Encodes a single `scalar` as an [ObjectIdScalar]. Automatically creates a mapping for a
    /// fresh object id if the term is not yet mapped.
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

    /// Decodes the entire `array` as a [PlainTermArray].
    fn decode_array(
        &self,
        array: &ObjectIdArray,
    ) -> Result<PlainTermArray, ObjectIdMappingError>;

    /// Decodes the entire `array` as a [TypedValueArray].
    fn decode_array_to_typed_value(
        &self,
        array: &ObjectIdArray,
    ) -> Result<TypedValueArray, ObjectIdMappingError>;

    /// Decodes a single `scalar` as a [PlainTermScalar].
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

    /// Decodes a single `scalar` as a [TypedValueScalar].
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
