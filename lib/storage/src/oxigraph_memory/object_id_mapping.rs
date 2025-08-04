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
    plain_term_encoding: PlainTermEncoding,
    next_id: AtomicU64,
    str_interning: DashSet<Arc<str>>,
    id2term: DashMap<EncodedObjectId, EncodedTerm, BuildHasherDefault<FxHasher>>,
    term2id: DashMap<EncodedTerm, EncodedObjectId, BuildHasherDefault<FxHasher>>,
}

impl MemoryObjectIdMapping {
    pub fn new(plain_term_encoding: PlainTermEncoding) -> Self {
        Self {
            plain_term_encoding,
            next_id: AtomicU64::new(0),
            str_interning: DashSet::new(),
            id2term: DashMap::with_hasher(BuildHasherDefault::default()),
            term2id: DashMap::with_hasher(BuildHasherDefault::default()),
        }
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
    fn encoding(&self) -> ObjectIdEncoding {
        ObjectIdEncoding::new(EncodedObjectId::SIZE)
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
                                self.encoding(),
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
                                self.encoding(),
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
                                        self.encoding(),
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
                                        self.encoding(),
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

        let mut result = ObjectIdArrayBuilder::new(self.encoding());
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;
    use rdf_fusion_encoding::object_id::ObjectIdArrayBuilder;
    use rdf_fusion_encoding::plain_term::{
        PlainTermArrayBuilder, PLAIN_TERM_ENCODING,
    };
    use rdf_fusion_encoding::{EncodingArray, EncodingScalar};
    use rdf_fusion_model::vocab::xsd;
    use rdf_fusion_model::{
        BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, QuadRef, TermRef,
    };

    #[test]
    fn test_encode_decode_roundtrip() -> DFResult<()> {
        let mapping = MemoryObjectIdMapping::new(PLAIN_TERM_ENCODING);
        let mut builder = PlainTermArrayBuilder::new(5);
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.com/a"));
        builder.append_blank_node(BlankNodeRef::new_unchecked("b1"));
        builder.append_literal(LiteralRef::new_typed_literal("hello", xsd::STRING));
        builder.append_literal(LiteralRef::new_language_tagged_literal_unchecked(
            "world", "en",
        ));
        builder.append_null();
        let plain_term_array = mapping
            .plain_term_encoding
            .try_new_array(builder.finish())?;

        let object_id_array = mapping.encode_array(&plain_term_array)?;
        let decoded_plain_term_array = mapping.decode_array(&object_id_array)?;

        assert_eq!(
            plain_term_array.array().len(),
            decoded_plain_term_array.array().len()
        );
        assert_eq!(
            plain_term_array.array().as_struct(),
            decoded_plain_term_array.array().as_struct()
        );

        Ok(())
    }

    #[test]
    fn test_id_uniqueness_and_consistency() -> DFResult<()> {
        let mapping = MemoryObjectIdMapping::new(PLAIN_TERM_ENCODING);
        let mut builder = PlainTermArrayBuilder::new(5);
        let nn1 = NamedNodeRef::new_unchecked("http://example.com/a");
        let nn2 = NamedNodeRef::new_unchecked("http://example.com/b");

        // Add two identical terms and one different one
        builder.append_named_node(nn1);
        builder.append_named_node(nn2);
        builder.append_named_node(nn1);
        let plain_term_array = mapping
            .plain_term_encoding
            .try_new_array(builder.finish())?;

        let object_id_array = mapping.encode_array(&plain_term_array)?;

        let id1 = object_id_array.object_ids().value(0);
        let id2 = object_id_array.object_ids().value(1);
        let id3 = object_id_array.object_ids().value(2);

        assert_eq!(id1, id3);
        assert_ne!(id1, id2);

        // Now encode again, the IDs should be the same
        let mut builder2 = PlainTermArrayBuilder::new(2);
        builder2.append_named_node(nn2);
        builder2.append_named_node(nn1);
        let plain_term_array2 = mapping
            .plain_term_encoding
            .try_new_array(builder2.finish())?;
        let object_id_array2 = mapping.encode_array(&plain_term_array2)?;

        let id4 = object_id_array2.object_ids().value(0);
        let id5 = object_id_array2.object_ids().value(1);

        assert_eq!(id2, id4);
        assert_eq!(id1, id5);

        Ok(())
    }

    #[test]
    fn test_try_get_object_id() -> DFResult<()> {
        let mapping = MemoryObjectIdMapping::new(PLAIN_TERM_ENCODING);

        let term1 = PlainTermScalar::from(TermRef::NamedNode(
            NamedNodeRef::new_unchecked("http://example.com/a"),
        ));
        let term2 =
            PlainTermScalar::from(TermRef::BlankNode(BlankNodeRef::new_unchecked("b1")));

        // Before encoding, should be None
        assert!(mapping.try_get_object_id(&term1)?.is_none());
        assert!(mapping.try_get_object_id(&term2)?.is_none());

        // Encode an array to populate the mapping
        let mut builder = PlainTermArrayBuilder::new(2);
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.com/a"));
        builder.append_blank_node(BlankNodeRef::new_unchecked("b1"));
        let plain_term_array = PLAIN_TERM_ENCODING.try_new_array(builder.finish())?;
        let object_id_array = mapping.encode_array(&plain_term_array)?;

        // After encoding, should be Some
        let object_id1 = mapping.try_get_object_id(&term1)?;
        assert!(object_id1.is_some());
        let object_id2 = mapping.try_get_object_id(&term2)?;
        assert!(object_id2.is_some());

        // Check if IDs match what's in the array
        assert_eq!(
            object_id1.unwrap().into_object_id().unwrap().as_ref(),
            object_id_array.object_ids().value(0)
        );
        assert_eq!(
            object_id2.unwrap().into_object_id().unwrap().as_ref(),
            object_id_array.object_ids().value(1)
        );

        // A term not in the mapping
        let term3 = PlainTermScalar::from(TermRef::NamedNode(
            NamedNodeRef::new_unchecked("http://example.com/c"),
        ));
        assert!(mapping.try_get_object_id(&term3)?.is_none());

        Ok(())
    }

    #[test]
    fn test_encode_quad() -> DFResult<()> {
        let mapping = MemoryObjectIdMapping::new(PLAIN_TERM_ENCODING);
        let quad = QuadRef {
            subject: NamedNodeRef::new_unchecked("http://example.com/s").into(),
            predicate: NamedNodeRef::new_unchecked("http://example.com/p").into(),
            object: LiteralRef::new_typed_literal("object", xsd::STRING).into(),
            graph_name: GraphNameRef::NamedNode(NamedNodeRef::new_unchecked(
                "http://example.com/g",
            )),
        };

        let object_id_quad = mapping.encode_quad(quad)?;

        // To verify, we can decode the IDs.
        // Let's build an array with the IDs and decode it.
        let mut builder = ObjectIdArrayBuilder::new(mapping.encoding());
        builder.append_object_id_bytes(object_id_quad.subject.as_ref())?;
        builder.append_object_id_bytes(object_id_quad.predicate.as_ref())?;
        builder.append_object_id_bytes(object_id_quad.object.as_ref())?;
        if let Some(graph_id) = object_id_quad.graph_name {
            builder.append_object_id_bytes(graph_id.as_ref())?;
        }
        let id_array = builder.finish();

        let decoded_array = mapping.decode_array(&id_array)?;

        let decoded_subject = decoded_array.try_as_scalar(0)?;
        let decoded_predicate = decoded_array.try_as_scalar(1)?;
        let decoded_object = decoded_array.try_as_scalar(2)?;

        assert_eq!(
            PlainTermScalar::from(quad.subject).into_scalar_value(),
            decoded_subject.into_scalar_value()
        );
        assert_eq!(
            PlainTermScalar::from(quad.predicate).into_scalar_value(),
            decoded_predicate.into_scalar_value()
        );
        assert_eq!(
            PlainTermScalar::from(quad.object).into_scalar_value(),
            decoded_object.into_scalar_value()
        );

        if !quad.graph_name.is_default_graph() {
            let decoded_graph = decoded_array.try_as_scalar(3)?;
            assert_eq!(
                PlainTermScalar::from_graph_name(quad.graph_name)?.into_scalar_value(),
                decoded_graph.into_scalar_value()
            );
        }

        Ok(())
    }
}
