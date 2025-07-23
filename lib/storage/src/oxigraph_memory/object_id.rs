#![allow(clippy::unreadable_literal)]

use std::fmt::Debug;
use std::hash::Hash;

/// The object id of the default graph.
pub(super) const DEFAULT_GRAPH_OBJECT_ID: u64 = 0;

#[derive(Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct ObjectId(u64);

impl ObjectId {
    /// Creates a new [ObjectId].
    pub fn new(value: u64) -> Self {
        ObjectId(value)
    }

    pub fn is_default_graph(self) -> bool {
        self.0 == DEFAULT_GRAPH_OBJECT_ID
    }
}

impl From<ObjectId> for u64 {
    fn from(value: ObjectId) -> Self {
        value.0
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct ObjectIdQuad {
    pub subject: ObjectId,
    pub predicate: ObjectId,
    pub object: ObjectId,
    pub graph_name: ObjectId,
}
