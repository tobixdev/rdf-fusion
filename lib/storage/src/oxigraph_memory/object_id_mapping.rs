#![allow(clippy::unreadable_literal)]

use crate::oxigraph_memory::object_id::{DEFAULT_GRAPH_OBJECT_ID, ObjectIdQuad};
use dashmap::DashMap;
use datafusion::common::{ScalarValue, exec_err};
use datafusion::error::DataFusionError;
use rdf_fusion_common::error::{CorruptionError, StorageError};
use rdf_fusion_common::{DFResult, ObjectId};
use rdf_fusion_encoding::object_id::{ObjectIdArray, ObjectIdMapping, ObjectIdScalar};
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermScalar};
use rdf_fusion_encoding::{EncodingScalar, TermEncoder};
use rdf_fusion_model::{
    GraphName, GraphNameRef, NamedOrBlankNode, QuadRef, Term, TermRef, ThinError,
    ThinResult,
};
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::sync::atomic::AtomicU64;

/// TODO
#[derive(Debug)]
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
    #[allow(clippy::same_name_method)]
    pub fn try_get_object_id<'term>(
        &self,
        term: impl Into<TermRef<'term>>,
    ) -> Option<ObjectId> {
        let term_ref = term.into().into_owned();
        self.term2id.get(&term_ref).map(|id| *id)
    }

    /// TODO
    pub fn try_get_object_id_for_graph_name(
        &self,
        graph: GraphNameRef<'_>,
    ) -> Option<ObjectId> {
        match graph {
            GraphNameRef::NamedNode(nn) => self.try_get_object_id(nn),
            GraphNameRef::BlankNode(bnode) => self.try_get_object_id(bnode),
            GraphNameRef::DefaultGraph => Some(DEFAULT_GRAPH_OBJECT_ID),
        }
    }

    /// TODO
    pub fn try_decode<TResolvable: Resolvable>(
        &self,
        object_id: ObjectId,
    ) -> Result<TResolvable, StorageError> {
        TResolvable::resolve(self, object_id)
    }

    /// TODO
    pub fn encode_term<'term>(&self, term: impl Into<TermRef<'term>>) -> ObjectId {
        let term = term.into().into_owned();

        if let Some(id) = self.term2id.get(&term) {
            return *id;
        }

        let id = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let id = ObjectId::from(id);

        self.id2term.insert(id, term.clone());
        self.term2id.insert(term, id);

        id
    }

    /// TODO
    pub fn encode_quad(&self, quad: QuadRef<'_>) -> ObjectIdQuad {
        ObjectIdQuad {
            graph_name: self.encode_graph_name(quad.graph_name),
            subject: self.encode_term(quad.subject),
            predicate: self.encode_term(quad.predicate),
            object: self.encode_term(quad.object),
        }
    }

    /// TODO
    pub fn encode_graph_name(&self, graph: GraphNameRef<'_>) -> ObjectId {
        match graph {
            GraphNameRef::NamedNode(nn) => self.encode_term(nn),
            GraphNameRef::BlankNode(bnode) => self.encode_term(bnode),
            GraphNameRef::DefaultGraph => DEFAULT_GRAPH_OBJECT_ID,
        }
    }

    fn decode_opt(
        &self,
        object_id: Option<ObjectId>,
    ) -> Result<ThinResult<Term>, StorageError> {
        match object_id {
            None => Ok(ThinError::expected()),
            Some(value) => {
                self.try_decode::<Option<Term>>(value)
                    .map(|term| match term {
                        None => ThinError::expected(),
                        Some(value) => Ok(value),
                    })
            }
        }
    }
}

impl Default for MemoryObjectIdMapping {
    fn default() -> Self {
        Self::new()
    }
}

impl ObjectIdMapping for MemoryObjectIdMapping {
    fn try_get_object_id(&self, id: TermRef<'_>) -> Option<ObjectId> {
        self.try_get_object_id(id).map(Into::into)
    }

    fn encode(&self, id: TermRef<'_>) -> ObjectId {
        self.encode_term(id).into()
    }

    fn decode_array(&self, array: &ObjectIdArray) -> DFResult<PlainTermArray> {
        let terms = array
            .object_ids()
            .iter()
            .map(|oid| {
                let oid = oid
                    .map(ObjectId::try_from)
                    .transpose()
                    .expect("Invalid object ID");
                self.decode_opt(oid)
            })
            .collect::<Result<Vec<ThinResult<Term>>, _>>();

        match terms {
            Ok(terms) => {
                DefaultPlainTermEncoder::encode_terms(terms.iter().map(|res| match res {
                    Ok(t) => Ok(t.as_ref()),
                    Err(err) => Err(*err),
                }))
            }
            Err(err) => Err(DataFusionError::External(Box::new(err))),
        }
    }

    fn decode_scalar(&self, scalar: &ObjectIdScalar) -> DFResult<PlainTermScalar> {
        match scalar.scalar_value() {
            ScalarValue::FixedSizeBinary(ObjectId::SIZE_I32, None) => {
                DefaultPlainTermEncoder::encode_term(ThinError::expected())
            }
            ScalarValue::FixedSizeBinary(ObjectId::SIZE_I32, Some(oid)) => {
                let oid =
                    ObjectId::try_from(oid.as_slice()).expect("Size already checked.");
                let term = self.try_decode::<Term>(oid);
                match term {
                    Ok(term) => DefaultPlainTermEncoder::encode_term(Ok(term.as_ref())),
                    Err(err) => Err(DataFusionError::External(Box::new(err))),
                }
            }
            _ => exec_err!("Unexpected scalar value in decode_scalar."),
        }
    }
}

pub trait Resolvable {
    fn resolve(
        mapping: &MemoryObjectIdMapping,
        object_id: ObjectId,
    ) -> Result<Self, StorageError>
    where
        Self: Sized;
}

impl Resolvable for Option<Term> {
    fn resolve(
        mapping: &MemoryObjectIdMapping,
        object_id: ObjectId,
    ) -> Result<Self, StorageError> {
        if object_id == DEFAULT_GRAPH_OBJECT_ID {
            return Ok(None);
        }
        mapping
            .id2term
            .get(&object_id)
            .map(|t| Some(t.as_ref().into_owned()))
            .ok_or(StorageError::Corruption(CorruptionError::msg(
                "Unmapped object ID.",
            )))
    }
}

impl Resolvable for Term {
    fn resolve(
        mapping: &MemoryObjectIdMapping,
        object_id: ObjectId,
    ) -> Result<Self, StorageError> {
        Option::<Term>::resolve(mapping, object_id).and_then(|t| {
            t.ok_or(StorageError::Corruption(CorruptionError::msg(
                "None term found.",
            )))
        })
    }
}

impl Resolvable for GraphName {
    fn resolve(
        mapping: &MemoryObjectIdMapping,
        object_id: ObjectId,
    ) -> Result<Self, StorageError> {
        if object_id == DEFAULT_GRAPH_OBJECT_ID {
            return Ok(GraphName::DefaultGraph);
        }

        Term::resolve(mapping, object_id).and_then(|t| match t {
            Term::NamedNode(nn) => Ok(GraphName::NamedNode(nn)),
            Term::BlankNode(bnode) => Ok(GraphName::BlankNode(bnode)),
            Term::Literal(_) => Err(StorageError::Corruption(CorruptionError::msg(
                "Unexpected literal term.",
            ))),
        })
    }
}

impl Resolvable for NamedOrBlankNode {
    fn resolve(
        mapping: &MemoryObjectIdMapping,
        object_id: ObjectId,
    ) -> Result<Self, StorageError> {
        Term::resolve(mapping, object_id).and_then(|t| match t {
            Term::NamedNode(nn) => Ok(NamedOrBlankNode::NamedNode(nn)),
            Term::BlankNode(bnode) => Ok(NamedOrBlankNode::BlankNode(bnode)),
            Term::Literal(_) => Err(StorageError::Corruption(CorruptionError::msg(
                "Unexpected literal term.",
            ))),
        })
    }
}
