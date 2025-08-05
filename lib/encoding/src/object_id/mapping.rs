use crate::object_id::{ObjectIdArray, ObjectIdEncoding, ObjectIdScalar};
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use crate::{EncodingArray, EncodingScalar};
use rdf_fusion_common::DFResult;
use std::fmt::Debug;

/// TODO
pub trait ObjectIdMapping: Debug + Send + Sync {
    /// Returns the encoding.
    fn encoding(&self) -> ObjectIdEncoding;

    /// TODO
    fn try_get_object_id(
        &self,
        scalar: &PlainTermScalar,
    ) -> DFResult<Option<ObjectIdScalar>>;

    /// TODO
    fn encode_array(&self, array: &PlainTermArray) -> DFResult<ObjectIdArray>;

    /// TODO
    fn encode_scalar(&self, scalar: &PlainTermScalar) -> DFResult<ObjectIdScalar> {
        let array = scalar.to_array(1)?;
        let encoded = self.encode_array(&array)?;
        encoded.try_as_scalar(0)
    }

    /// TODO
    fn decode_array(&self, array: &ObjectIdArray) -> DFResult<PlainTermArray>;

    /// TODO
    fn decode_scalar(&self, scalar: &ObjectIdScalar) -> DFResult<PlainTermScalar> {
        let array = scalar.to_array(1)?;
        let decoded = self.decode_array(&array)?;
        decoded.try_as_scalar(0)
    }
}
