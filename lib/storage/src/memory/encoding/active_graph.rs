use crate::memory::object_id::EncodedGraphObjectId;

/// An encoded version of the active graph.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum EncodedActiveGraph {
    #[default]
    DefaultGraph,
    AllGraphs,
    Union(Vec<EncodedGraphObjectId>),
    AnyNamedGraph,
}
