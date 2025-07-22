#![allow(clippy::unreadable_literal)]

use crate::oxigraph_memory::object_id::{ObjectId, ObjectIdQuad, DEFAULT_GRAPH_OBJECT_ID};
use dashmap::DashMap;
use rdf_fusion_common::error::{CorruptionError, StorageError};
use rdf_fusion_model::{GraphName, GraphNameRef, NamedOrBlankNode, QuadRef, Term, TermRef};
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::sync::atomic::AtomicU64;

/// TODO
pub struct MemoryObjectIdMapping {
    next_id: AtomicU64,

    id2term: DashMap<ObjectId, Term, BuildHasherDefault<FxHasher>>,
    term2id: DashMap<Term, ObjectId, BuildHasherDefault<FxHasher>>,
}

/// We reserve some object IDs for special purposes.
///
/// - 0: Default Graph
const FIRST_REGULAR_OBJECT_ID: u64 = 1;

impl MemoryObjectIdMapping {
    /// TODO
    pub fn new() -> Self {
        MemoryObjectIdMapping {
            next_id: AtomicU64::new(FIRST_REGULAR_OBJECT_ID),
            id2term: DashMap::with_hasher(BuildHasherDefault::default()),
            term2id: DashMap::with_hasher(BuildHasherDefault::default()),
        }
    }

    /// TODO
    pub fn try_get_object_id<'term>(&self, term: impl Into<TermRef<'term>>) -> Option<ObjectId> {
        let term_ref = term.into().into_owned();
        self.term2id.get(&term_ref).map(|id| *id)
    }

    /// TODO
    pub fn try_get_object_id_for_graph_name(&self, graph: GraphNameRef<'_>) -> Option<ObjectId> {
        match graph {
            GraphNameRef::NamedNode(nn) => self.try_get_object_id(nn),
            GraphNameRef::BlankNode(bnode) => self.try_get_object_id(bnode),
            GraphNameRef::DefaultGraph => Some(ObjectId::new(DEFAULT_GRAPH_OBJECT_ID)),
        }
    }

    /// TODO
    pub fn try_decode<TDecodable: ObjectIdDecodable>(
        &self,
        object_id: ObjectId,
    ) -> Result<TDecodable, StorageError> {
        TDecodable::decode(self, object_id)
    }

    /// TODO
    pub fn encode<'term>(&self, term: impl Into<TermRef<'term>>) -> ObjectId {
        let term = term.into().into_owned();

        if let Some(id) = self.term2id.get(&term) {
            return *id;
        }

        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let id = ObjectId::new(id);

        self.id2term.insert(id.clone(), term.clone());
        self.term2id.insert(term, id.clone());

        id
    }

    /// TODO
    pub fn encode_quad(&self, quad: QuadRef<'_>) -> ObjectIdQuad {
        ObjectIdQuad {
            graph_name: self.encode_graph_name(quad.graph_name),
            subject: self.encode(quad.subject),
            predicate: self.encode(quad.predicate),
            object: self.encode(quad.object),
        }
    }

    /// TODO
    pub fn encode_graph_name(&self, graph: GraphNameRef<'_>) -> ObjectId {
        match graph {
            GraphNameRef::NamedNode(nn) => self.encode(nn),
            GraphNameRef::BlankNode(bnode) => self.encode(bnode),
            GraphNameRef::DefaultGraph => ObjectId::new(DEFAULT_GRAPH_OBJECT_ID),
        }
    }
}

pub trait ObjectIdDecodable {
    fn decode(mapping: &MemoryObjectIdMapping, object_id: ObjectId) -> Result<Self, StorageError>
    where
        Self: Sized;
}

impl ObjectIdDecodable for Term {
    fn decode(mapping: &MemoryObjectIdMapping, object_id: ObjectId) -> Result<Self, StorageError> {
        mapping
            .id2term
            .get(&object_id)
            .map(|t| t.as_ref().into_owned())
            .ok_or(StorageError::Corruption(CorruptionError::msg(
                "Unmapped object ID.",
            )))
    }
}

impl ObjectIdDecodable for GraphName {
    fn decode(mapping: &MemoryObjectIdMapping, object_id: ObjectId) -> Result<Self, StorageError> {
        if object_id.is_default_graph() {
            return Ok(GraphName::DefaultGraph);
        }

        Term::decode(mapping, object_id).and_then(|t| match t {
            Term::NamedNode(nn) => Ok(GraphName::NamedNode(nn)),
            Term::BlankNode(bnode) => Ok(GraphName::BlankNode(bnode)),
            Term::Literal(_) => Err(StorageError::Corruption(CorruptionError::msg(
                "Unexpected literal term.",
            ))),
        })
    }
}

impl ObjectIdDecodable for NamedOrBlankNode {
    fn decode(mapping: &MemoryObjectIdMapping, object_id: ObjectId) -> Result<Self, StorageError> {
        Term::decode(mapping, object_id).and_then(|t| match t {
            Term::NamedNode(nn) => Ok(NamedOrBlankNode::NamedNode(nn)),
            Term::BlankNode(bnode) => Ok(NamedOrBlankNode::BlankNode(bnode)),
            Term::Literal(_) => Err(StorageError::Corruption(CorruptionError::msg(
                "Unexpected literal term.",
            ))),
        })
    }
}
