use crate::object_id::ObjectIdArray;
use crate::plain_term::PlainTermArray;
use rdf_fusion_common::DFResult;
use rdf_fusion_model::TermRef;
use std::fmt::Debug;

/// TODO
pub trait ObjectIdMapping: Debug + Send + Sync {
    /// TODO
    fn try_get_object_id(&self, id: TermRef<'_>) -> Option<u64>;

    /// TODO
    fn encode(&self, id: TermRef<'_>) -> u64;

    /// TODO
    fn decode_array(&self, array: &ObjectIdArray) -> DFResult<PlainTermArray>;
}
