#![allow(clippy::unreadable_literal)]

use rdf_fusion_common::ObjectId;
use std::fmt::Debug;
use std::hash::Hash;

/// The object id of the default graph.
pub(super) const DEFAULT_GRAPH_OBJECT_ID: ObjectId = ObjectId::from_u64(0);

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct ObjectIdQuad {
    pub subject: ObjectId,
    pub predicate: ObjectId,
    pub object: ObjectId,
    pub graph_name: ObjectId,
}
