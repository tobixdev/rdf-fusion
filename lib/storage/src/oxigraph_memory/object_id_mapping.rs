#![allow(clippy::unreadable_literal)]

use crate::oxigraph_memory::object_id::{EncodedObjectId, ObjectIdQuad};
use dashmap::DashMap;
use datafusion::common::{exec_err, ScalarValue};
use datafusion::error::DataFusionError;
use rdf_fusion_common::error::StorageError;
use rdf_fusion_common::{DFResult, ObjectId};
use rdf_fusion_encoding::object_id::{ObjectIdArray, ObjectIdMapping, ObjectIdScalar};
use rdf_fusion_encoding::plain_term::encoders::DefaultPlainTermEncoder;
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermScalar};
use rdf_fusion_encoding::{EncodingScalar, TermEncoder};
use rdf_fusion_model::{GraphNameRef, QuadRef, Term, TermRef, ThinError, ThinResult};
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::sync::atomic::AtomicU64;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
enum TermType {
    NamedNode,
    BlankNode,
    TypedLiteral,
    LangString,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
struct StrId([u8; 5]);

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
struct EncodedTerm(TermType, StrId, StrId);

/// TODO
#[derive(Debug)]
pub struct MemoryObjectIdMapping {
    next_id: AtomicU64,

    id2str: DashMap<StrId, String, BuildHasherDefault<FxHasher>>,
    str2id: DashMap<String, StrId, BuildHasherDefault<FxHasher>>,

    id2term: DashMap<EncodedObjectId, EncodedTerm, BuildHasherDefault<FxHasher>>,
    term2id: DashMap<EncodedTerm, EncodedObjectId, BuildHasherDefault<FxHasher>>,
}

impl MemoryObjectIdMapping {
    pub fn new() -> Self {
        Self {
            next_id: AtomicU64::new(0),
            id2str: DashMap::with_hasher(BuildHasherDefault::default()),
            str2id: DashMap::with_hasher(BuildHasherDefault::default()),
            id2term: DashMap::with_hasher(BuildHasherDefault::default()),
            term2id: DashMap::with_hasher(BuildHasherDefault::default()),
        }
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
    pub fn encode_graph_name(&self, graph: GraphNameRef<'_>) -> Option<ObjectId> {
        match graph {
            GraphNameRef::NamedNode(nn) => Some(self.encode_term(nn)),
            GraphNameRef::BlankNode(bnode) => Some(self.encode_term(bnode)),
            GraphNameRef::DefaultGraph => None,
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

impl ObjectIdMapping for MemoryObjectIdMapping {
    fn object_id_len(&self) -> u8 {
        EncodedObjectId::OBJECT_ID_SIZE as u8
    }

    fn encode(&self, id: TermRef<'_>) -> ObjectId {
        self.encode_term(id).into()
    }

    fn encode_scalar(&self, scalar: PlainTermScalar) -> ObjectIdScalar {
        todo!()
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
            ScalarValue::FixedSizeBinary(EncodedObjectId::OBJECT_ID_SIZE_I32, None) => {
                DefaultPlainTermEncoder::encode_term(ThinError::expected())
            }
            ScalarValue::FixedSizeBinary(EncodedObjectId::OBJECT_ID_SIZE_I32, Some(oid)) => {
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
