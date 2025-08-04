#![allow(clippy::unreadable_literal)]

use crate::oxigraph_memory::object_id::{EncodedObjectId, ObjectIdQuad};
use dashmap::{DashMap, DashSet};
use datafusion::arrow::array::Array;
use datafusion::common::exec_err;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::{
    ObjectIdArray, ObjectIdArrayBuilder, ObjectIdEncoding, ObjectIdMapping,
    ObjectIdScalar,
};
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayBuilder, PlainTermEncoding, PlainTermScalar,
};
use rdf_fusion_encoding::{EncodingArray, TermDecoder, TermEncoding};
use rdf_fusion_model::{BlankNodeRef, LiteralRef, NamedNodeRef, QuadRef, TermRef};
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
enum TermType {
    /// Only a single relevant string pointer: The IRI
    NamedNode,
    /// Only a single relevant string pointer: The BNode id
    BlankNode,
    /// Two relevant string pointers: The value and the data type
    TypedLiteral,
    /// Two relevant string pointers: The value and the language
    LangString,
}

#[derive(Debug, Hash, PartialEq, Eq, Clone, PartialOrd, Ord)]
struct EncodedTerm(TermType, Arc<str>, Option<Arc<str>>);

/// TODO
#[derive(Debug)]
pub struct MemoryObjectIdMapping {
    oid_encoding: ObjectIdEncoding,
    plain_term_encoding: PlainTermEncoding,
    next_id: AtomicU64,
    str_interning: DashSet<Arc<str>>,
    id2term: DashMap<EncodedObjectId, EncodedTerm, BuildHasherDefault<FxHasher>>,
    term2id: DashMap<EncodedTerm, EncodedObjectId, BuildHasherDefault<FxHasher>>,
}

impl MemoryObjectIdMapping {
    pub fn try_new(
        encoding: ObjectIdEncoding,
        plain_term_encoding: PlainTermEncoding,
    ) -> DFResult<Self> {
        if encoding.object_id_len() != EncodedObjectId::SIZE {
            panic!("Invalid object ID encoding");
        }

        Ok(Self {
            oid_encoding: encoding,
            plain_term_encoding,
            next_id: AtomicU64::new(0),
            str_interning: DashSet::new(),
            id2term: DashMap::with_hasher(BuildHasherDefault::default()),
            term2id: DashMap::with_hasher(BuildHasherDefault::default()),
        })
    }

    pub fn encoding(&self) -> &ObjectIdEncoding {
        &self.oid_encoding
    }

    /// TODO
    pub fn encode_quad(&self, quad: QuadRef<'_>) -> DFResult<ObjectIdQuad> {
        Ok(ObjectIdQuad {
            graph_name: self
                .encode_scalar(&PlainTermScalar::from_graph_name(quad.graph_name)?)
                .map(|scalar| scalar.into_object_id())?,
            subject: self
                .encode_scalar(&PlainTermScalar::from(quad.subject))?
                .into_object_id()
                .expect("Input is never none"),
            predicate: self
                .encode_scalar(&PlainTermScalar::from(quad.predicate))?
                .into_object_id()
                .expect("Input is never none"),
            object: self
                .encode_scalar(&PlainTermScalar::from(quad.object))?
                .into_object_id()
                .expect("Input is never none"),
        })
    }

    fn intern_str(&self, value: &str) -> Arc<str> {
        let found = self.str_interning.get(value);
        match found {
            None => {
                let result = Arc::<str>::from(value);
                self.str_interning.insert(result.clone());
                result
            }
            Some(entry) => entry.clone(),
        }
    }

    fn obtain_object_id(&self, encoded_term: &EncodedTerm) -> EncodedObjectId {
        let found = self.term2id.get(encoded_term);
        match found {
            None => {
                let next_id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let object_id =
                    EncodedObjectId::try_from(next_id).expect("Invalid object ID");
                self.id2term.insert(object_id, encoded_term.clone());
                self.term2id.insert(encoded_term.clone(), object_id);
                object_id
            }
            Some(entry) => entry.clone(),
        }
    }
}

impl ObjectIdMapping for MemoryObjectIdMapping {
    fn object_id_len(&self) -> u8 {
        EncodedObjectId::SIZE
    }

    fn try_get_object_id(
        &self,
        scalar: &PlainTermScalar,
    ) -> DFResult<Option<ObjectIdScalar>> {
        let term = DefaultPlainTermDecoder::decode_term(scalar);
        Ok(match term {
            Ok(term) => match term {
                TermRef::NamedNode(nn) => match self.str_interning.get(nn.as_str()) {
                    None => None,
                    Some(value) => {
                        let encoded_term =
                            EncodedTerm(TermType::NamedNode, value.clone(), None);
                        self.term2id.get(&encoded_term).map(|oid| {
                            ObjectIdScalar::from_object_id(
                                self.oid_encoding.clone(),
                                (*oid.value()).into(),
                            )
                        })
                    }
                },
                TermRef::BlankNode(bnode) => match self.str_interning.get(bnode.as_str())
                {
                    None => None,
                    Some(value) => {
                        let encoded_term =
                            EncodedTerm(TermType::BlankNode, value.clone(), None);
                        self.term2id.get(&encoded_term).map(|oid| {
                            ObjectIdScalar::from_object_id(
                                self.oid_encoding.clone(),
                                (*oid.value()).into(),
                            )
                        })
                    }
                },
                TermRef::Literal(lit) => {
                    if let Some(language) = lit.language() {
                        match (
                            self.str_interning.get(lit.value()),
                            self.str_interning.get(language),
                        ) {
                            (Some(value), Some(language)) => {
                                let encoded_term = EncodedTerm(
                                    TermType::LangString,
                                    value.clone(),
                                    Some(language.clone()),
                                );
                                self.term2id.get(&encoded_term).map(|oid| {
                                    ObjectIdScalar::from_object_id(
                                        self.oid_encoding.clone(),
                                        (*oid.value()).into(),
                                    )
                                })
                            }
                            _ => None,
                        }
                    } else {
                        match (
                            self.str_interning.get(lit.value()),
                            self.str_interning.get(lit.datatype().as_str()),
                        ) {
                            (Some(value), Some(data_type)) => {
                                let encoded_term = EncodedTerm(
                                    TermType::TypedLiteral,
                                    value.clone(),
                                    Some(data_type.clone()),
                                );
                                self.term2id.get(&encoded_term).map(|oid| {
                                    ObjectIdScalar::from_object_id(
                                        self.oid_encoding.clone(),
                                        (*oid.value()).into(),
                                    )
                                })
                            }
                            _ => None,
                        }
                    }
                }
            },
            Err(_) => None,
        })
    }

    fn encode_array(&self, array: &PlainTermArray) -> DFResult<ObjectIdArray> {
        let terms = DefaultPlainTermDecoder::decode_terms(array);

        let mut result = ObjectIdArrayBuilder::new(self.oid_encoding.clone());
        for term in terms {
            match term {
                Ok(term) => match term {
                    TermRef::NamedNode(nn) => {
                        let nn = self.intern_str(nn.as_str());
                        let encoded_term = EncodedTerm(TermType::NamedNode, nn, None);
                        let object_id = self.obtain_object_id(&encoded_term);
                        result.append_object_id_bytes(object_id.as_ref())?;
                    }
                    TermRef::BlankNode(bnode) => {
                        let nn = self.intern_str(bnode.as_str());
                        let encoded_term = EncodedTerm(TermType::BlankNode, nn, None);
                        let object_id = self.obtain_object_id(&encoded_term);
                        result.append_object_id_bytes(object_id.as_ref())?;
                    }
                    TermRef::Literal(lit) => {
                        if let Some(language) = lit.language() {
                            let value = self.intern_str(lit.value());
                            let language = self.intern_str(language);
                            let encoded_term =
                                EncodedTerm(TermType::LangString, value, Some(language));
                            let object_id = self.obtain_object_id(&encoded_term);
                            result.append_object_id_bytes(object_id.as_ref())?;
                        } else {
                            let value = self.intern_str(lit.value());
                            let data_type = self.intern_str(lit.datatype().as_str());
                            let encoded_term = EncodedTerm(
                                TermType::TypedLiteral,
                                value,
                                Some(data_type),
                            );
                            let object_id = self.obtain_object_id(&encoded_term);
                            result.append_object_id_bytes(object_id.as_ref())?;
                        }
                    }
                },
                Err(_) => result.append_null(),
            }
        }

        Ok(result.finish())
    }

    fn decode_array(&self, array: &ObjectIdArray) -> DFResult<PlainTermArray> {
        let terms = array.object_ids().iter().map(|oid| {
            let oid = oid
                .map(EncodedObjectId::try_from)
                .transpose()
                .expect("Invalid object ID");
            oid.map(|oid| self.id2term.get(&oid).expect("Missing object id").clone())
        });

        let mut builder = PlainTermArrayBuilder::new(array.array().len());
        for term in terms {
            match term {
                Some(EncodedTerm(TermType::NamedNode, value, _)) => {
                    builder
                        .append_named_node(NamedNodeRef::new_unchecked(value.as_ref()));
                }
                Some(EncodedTerm(TermType::BlankNode, value, _)) => {
                    builder
                        .append_blank_node(BlankNodeRef::new_unchecked(value.as_ref()));
                }
                Some(EncodedTerm(TermType::TypedLiteral, value, Some(data_type))) => {
                    let data_type = NamedNodeRef::new_unchecked(data_type.as_ref());
                    builder.append_literal(LiteralRef::new_typed_literal(
                        value.as_ref(),
                        data_type,
                    ));
                }
                Some(EncodedTerm(TermType::LangString, value, Some(language))) => builder
                    .append_literal(LiteralRef::new_language_tagged_literal_unchecked(
                        value.as_ref(),
                        language.as_ref(),
                    )),
                None => builder.append_null(),
                _ => return exec_err!("Invalid object id encoding"),
            }
        }

        self.plain_term_encoding.try_new_array(builder.finish())
    }
}
