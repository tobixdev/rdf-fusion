use crate::object_id::{ObjectIdArray, ObjectIdScalar};
use crate::plain_term::{PlainTermArray, PlainTermScalar};
use rdf_fusion_common::DFResult;
use std::fmt::Debug;

/// TODO
pub trait ObjectIdMapping: Debug + Send + Sync {
    /// Returns the size of the object id.
    fn object_id_len(&self) -> u8;

    /// TODO
    fn encode(&self, array: PlainTermArray) -> ObjectIdArray;

    /// TODO
    fn encode_scalar(&self, scalar: PlainTermScalar) -> ObjectIdScalar;

    /// TODO
    fn decode_array(&self, array: &ObjectIdArray) -> DFResult<PlainTermArray>;

    /// TODO
    fn decode_scalar(&self, scalar: &ObjectIdScalar) -> DFResult<PlainTermScalar>;
}
