#![allow(clippy::unreadable_literal)]

use crate::index::EncodedQuad;
use crate::memory::encoding::{EncodedTerm, EncodedTypedValue};
use crate::memory::object_id::{DEFAULT_GRAPH_ID, EncodedGraphObjectId, EncodedObjectId};
use dashmap::{DashMap, DashSet};
use datafusion::arrow::array::{Array, FixedSizeBinaryArray, FixedSizeBinaryBuilder};
use rdf_fusion_encoding::object_id::{
    ObjectId, ObjectIdArray, ObjectIdMapping, ObjectIdMappingError, ObjectIdSize,
};
use rdf_fusion_encoding::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayElementBuilder, PlainTermScalar,
};
use rdf_fusion_encoding::typed_value::{
    TypedValueArray, TypedValueArrayElementBuilder, TypedValueEncodingRef,
};
use rdf_fusion_encoding::{EncodingArray, TermDecoder};
use rdf_fusion_model::DFResult;
use rdf_fusion_model::{
    BlankNodeRef, GraphNameRef, LiteralRef, NamedNodeRef, NamedOrBlankNode, QuadRef,
    Term, TermRef, TypedValueRef,
};
use rustc_hash::FxHasher;
use std::hash::BuildHasherDefault;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

/// Maintains a mapping between RDF terms and object IDs in memory.
///
/// The mapping happens on two levels: first, all strings are interned, second, the encoded term
/// that refers to the interned strings is mapped to an encoded Object ID.
///
/// # Object IDs
///
/// The encoded Object ID is a 32-bit unsigned integer used to uniquely identify RDF terms.
/// Currently, we simply use a counter to allocate new object IDs.
///
/// # Typed Values
///
/// In addition to mapping object ids to terms, we also maintain a mapping to the typed value to
/// speed-up queries working on typed values.
#[derive(Debug)]
pub struct MemObjectIdMapping {
    /// Contains the next free object id.
    next_id: AtomicU32,
    /// A set for interning strings.
    str_interning: DashSet<Arc<str>>,
    /// Maps object ids to the encoded terms & typed values.
    id2term: DashMap<
        EncodedObjectId,
        (EncodedTerm, EncodedTypedValue),
        BuildHasherDefault<FxHasher>,
    >,
    /// Maps terms to their object id.
    ///
    /// A lookup from typed values to the object id of the "canonical" term is only possible by
    /// first converting the typed value to term.
    term2id: DashMap<EncodedTerm, EncodedObjectId, BuildHasherDefault<FxHasher>>,
}

impl Default for MemObjectIdMapping {
    fn default() -> Self {
        Self::new()
    }
}

impl MemObjectIdMapping {
    /// Creates a new empty [MemObjectIdMapping].
    ///
    /// The given encodings are used for creating the outputs of the mapping.
    pub fn new() -> Self {
        Self {
            next_id: AtomicU32::new(1), // Start at 1 to account for Default Graph.
            str_interning: DashSet::new(),
            id2term: DashMap::with_hasher(BuildHasherDefault::default()),
            term2id: DashMap::with_hasher(BuildHasherDefault::default()),
        }
    }

    pub(super) fn encode_graph_name_intern(
        &self,
        scalar: GraphNameRef<'_>,
    ) -> EncodedGraphObjectId {
        match scalar {
            GraphNameRef::NamedNode(nn) => {
                EncodedGraphObjectId(self.encode_term_intern(nn))
            }
            GraphNameRef::BlankNode(bnode) => {
                EncodedGraphObjectId(self.encode_term_intern(bnode))
            }
            GraphNameRef::DefaultGraph => DEFAULT_GRAPH_ID,
        }
    }

    pub(super) fn encode_term_intern<'term>(
        &self,
        scalar: impl Into<TermRef<'term>>,
    ) -> EncodedObjectId {
        let scalar = scalar.into();
        let term = self.obtain_encoded_term(scalar);
        self.obtain_object_id(&term)
    }

    /// Encodes the entire `quad`.
    pub(super) fn encode_quad(
        &self,
        quad: QuadRef<'_>,
    ) -> DFResult<EncodedQuad<EncodedObjectId>> {
        Ok(EncodedQuad {
            graph_name: self.encode_graph_name_intern(quad.graph_name).0,
            subject: self.encode_term_intern(quad.subject),
            predicate: self.encode_term_intern(quad.predicate),
            object: self.encode_term_intern(quad.object),
        })
    }

    /// Decodes the given `object_id`.
    ///
    /// # Error
    ///
    /// Returns an error if the object id is unknown.
    pub(super) fn decode_term(
        &self,
        object_id: EncodedObjectId,
    ) -> Result<Term, ObjectIdMappingError> {
        let term = self
            .try_get_encoded_term_from_object_id(object_id)
            .ok_or(ObjectIdMappingError::UnknownObjectId)?;
        Ok(TermRef::from(&term).into_owned())
    }

    /// Decodes the given `object_id`.
    ///
    /// # Error
    ///
    /// Returns an error if the object id is unknown or if `object_id` referred to the default
    /// graph or a literal.
    pub(super) fn decode_named_graph(
        &self,
        term: EncodedObjectId,
    ) -> Result<NamedOrBlankNode, ObjectIdMappingError> {
        match self.decode_term(term)? {
            Term::NamedNode(nn) => Ok(NamedOrBlankNode::NamedNode(nn)),
            Term::BlankNode(bnode) => Ok(NamedOrBlankNode::BlankNode(bnode)),
            Term::Literal(_) => Err(ObjectIdMappingError::LiteralAsGraphName),
        }
    }

    /// Tries to get an [EncodedTerm] from the given `term`.
    pub(super) fn try_get_encoded_term(&self, term: TermRef<'_>) -> Option<EncodedTerm> {
        match term {
            TermRef::NamedNode(nn) => self
                .str_interning
                .get(nn.as_str())
                .map(|value| EncodedTerm::NamedNode(value.clone())),
            TermRef::BlankNode(bnode) => self
                .str_interning
                .get(bnode.as_str())
                .map(|value| EncodedTerm::BlankNode(value.clone())),
            TermRef::Literal(lit) => {
                if let Some(language) = lit.language() {
                    match (
                        self.str_interning.get(lit.value()),
                        self.str_interning.get(language),
                    ) {
                        (Some(value), Some(language)) => {
                            Some(EncodedTerm::LangString(value.clone(), language.clone()))
                        }
                        _ => None,
                    }
                } else {
                    match (
                        self.str_interning.get(lit.value()),
                        self.str_interning.get(lit.datatype().as_str()),
                    ) {
                        (Some(value), Some(data_type)) => Some(
                            EncodedTerm::TypedLiteral(value.clone(), data_type.clone()),
                        ),
                        _ => None,
                    }
                }
            }
        }
    }

    pub(super) fn obtain_encoded_term(&self, term: TermRef<'_>) -> EncodedTerm {
        match term {
            TermRef::NamedNode(nn) => {
                let arc = self.intern_str(nn.as_str());
                EncodedTerm::NamedNode(arc)
            }
            TermRef::BlankNode(bnode) => {
                let arc = self.intern_str(bnode.as_str());
                EncodedTerm::BlankNode(arc)
            }
            TermRef::Literal(lit) => {
                if let Some(language) = lit.language() {
                    let value = self.intern_str(lit.value());
                    let language = self.intern_str(language);
                    EncodedTerm::LangString(value, language)
                } else {
                    let value = self.intern_str(lit.value());
                    let datatype = self.intern_str(lit.datatype().as_str());
                    EncodedTerm::TypedLiteral(value, datatype)
                }
            }
        }
    }

    pub(super) fn try_get_encoded_object_id_from_term<'term>(
        &self,
        encoded_term: impl Into<TermRef<'term>>,
    ) -> Option<EncodedObjectId> {
        let encoded_term = encoded_term.into();
        self.try_get_encoded_term(encoded_term)
            .and_then(|term| self.try_get_encoded_object_id(&term))
    }

    pub(super) fn try_get_encoded_object_id_from_graph_name(
        &self,
        encoded_term: GraphNameRef<'_>,
    ) -> Option<EncodedGraphObjectId> {
        match encoded_term {
            GraphNameRef::NamedNode(nn) => self
                .try_get_encoded_object_id_from_term(TermRef::from(nn))
                .map(EncodedGraphObjectId),
            GraphNameRef::BlankNode(bnode) => self
                .try_get_encoded_object_id_from_term(TermRef::from(bnode))
                .map(EncodedGraphObjectId),
            GraphNameRef::DefaultGraph => Some(DEFAULT_GRAPH_ID),
        }
    }

    pub(super) fn try_get_encoded_object_id(
        &self,
        encoded_term: &EncodedTerm,
    ) -> Option<EncodedObjectId> {
        self.term2id.get(encoded_term).map(|entry| *entry)
    }

    pub(super) fn try_get_encoded_term_from_object_id(
        &self,
        object_id: EncodedObjectId,
    ) -> Option<EncodedTerm> {
        self.id2term.get(&object_id).map(|entry| {
            let (term, _) = entry.value();
            term.clone()
        })
    }

    pub(super) fn try_get_encoded_typed_value_from_object_id(
        &self,
        object_id: EncodedObjectId,
    ) -> Option<EncodedTypedValue> {
        self.id2term.get(&object_id).map(|entry| {
            let (_, typed_value) = entry.value();
            typed_value.clone()
        })
    }

    pub(super) fn obtain_object_id(&self, encoded_term: &EncodedTerm) -> EncodedObjectId {
        let found = self.term2id.get(encoded_term);
        match found {
            None => {
                let next_id = self.next_id.fetch_add(1, Ordering::Relaxed);
                let object_id = EncodedObjectId::from(next_id);
                let encoded_typed_value = EncodedTypedValue::from(encoded_term);
                self.id2term
                    .insert(object_id, (encoded_term.clone(), encoded_typed_value));
                self.term2id.insert(encoded_term.clone(), object_id);
                object_id
            }
            Some(entry) => *entry,
        }
    }

    fn intern_str(&self, value: &str) -> Arc<str> {
        let found = self.str_interning.get(value);
        match found {
            None => {
                let result = Arc::<str>::from(value);
                self.str_interning.insert(Arc::clone(&result));
                result
            }
            Some(entry) => entry.clone(),
        }
    }
}

impl ObjectIdMapping for MemObjectIdMapping {
    fn object_id_size(&self) -> ObjectIdSize {
        ObjectIdSize::try_from(4).expect("4 is valid")
    }

    fn try_get_object_id(
        &self,
        scalar: &PlainTermScalar,
    ) -> Result<Option<ObjectId>, ObjectIdMappingError> {
        let term = DefaultPlainTermDecoder::decode_term(scalar);
        let result = term
            .ok()
            .and_then(|term| self.try_get_encoded_term(term))
            .and_then(|term| self.try_get_encoded_object_id(&term))
            .map(|oid| oid.as_object_id());
        Ok(result)
    }

    fn encode_array(
        &self,
        array: &PlainTermArray,
    ) -> Result<FixedSizeBinaryArray, ObjectIdMappingError> {
        let terms = DefaultPlainTermDecoder::decode_terms(array);

        // TODO: without alloc/Arc copy
        let mut result = FixedSizeBinaryBuilder::new(4);
        for term in terms {
            match term {
                Ok(term) => {
                    let encoded_term = self.obtain_encoded_term(term);
                    let object_id = self.obtain_object_id(&encoded_term);
                    result.append_value(&object_id.as_bytes())?;
                }
                Err(_) => result.append_null(),
            }
        }

        Ok(result.finish())
    }

    fn decode_array(
        &self,
        array: &ObjectIdArray,
    ) -> Result<PlainTermArray, ObjectIdMappingError> {
        if array.object_ids().value_length() != 4 {
            return Err(ObjectIdMappingError::UnknownObjectIdEncoding);
        }

        let terms = array.object_ids().iter().map(|oid| {
            oid.map(|oid| {
                let oid = EncodedObjectId::try_from(oid).expect("4 Bytes checked above");
                self.try_get_encoded_term_from_object_id(oid)
                    .expect("Missing EncodedObjectId")
                    .clone()
            })
        });

        // TODO: can we remove the clone?
        let mut builder = PlainTermArrayElementBuilder::new(array.array().len());
        for term in terms {
            match term {
                Some(EncodedTerm::NamedNode(value)) => {
                    builder
                        .append_named_node(NamedNodeRef::new_unchecked(value.as_ref()));
                }
                Some(EncodedTerm::BlankNode(value)) => {
                    builder
                        .append_blank_node(BlankNodeRef::new_unchecked(value.as_ref()));
                }
                Some(EncodedTerm::TypedLiteral(value, data_type)) => {
                    let data_type = NamedNodeRef::new_unchecked(data_type.as_ref());
                    builder.append_literal(LiteralRef::new_typed_literal(
                        value.as_ref(),
                        data_type,
                    ));
                }
                Some(EncodedTerm::LangString(value, language)) => builder.append_literal(
                    LiteralRef::new_language_tagged_literal_unchecked(
                        value.as_ref(),
                        language.as_ref(),
                    ),
                ),
                None => builder.append_null(),
            }
        }

        Ok(builder.finish())
    }

    fn decode_array_to_typed_value(
        &self,
        encoding: &TypedValueEncodingRef,
        array: &ObjectIdArray,
    ) -> Result<TypedValueArray, ObjectIdMappingError> {
        if array.object_ids().value_length() != 4 {
            return Err(ObjectIdMappingError::UnknownObjectIdEncoding);
        }

        let typed_values = array.object_ids().iter().map(|oid| {
            oid.map(|oid| {
                let oid = EncodedObjectId::try_from(oid).expect("4 Bytes checked above");
                self.try_get_encoded_typed_value_from_object_id(oid)
                    .expect("Missing EncodedObjectId")
                    .clone()
            })
        });

        // TODO: can we remove the clone?
        let mut builder = TypedValueArrayElementBuilder::new(Arc::clone(encoding));
        for typed_value in typed_values {
            let typed_value =
                typed_value.as_ref().and_then(Option::<TypedValueRef>::from);
            match typed_value {
                None => builder.append_null()?,
                Some(typed_value) => builder.append_typed_value(typed_value)?,
            }
        }

        Ok(builder.finish())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::AsArray;
    use rdf_fusion_encoding::EncodingArray;
    use rdf_fusion_encoding::object_id::ObjectIdEncoding;
    use rdf_fusion_encoding::plain_term::PlainTermArrayElementBuilder;
    use rdf_fusion_model::vocab::xsd;
    use rdf_fusion_model::{BlankNodeRef, LiteralRef, NamedNodeRef, TermRef};

    #[test]
    fn test_encode_decode_roundtrip() -> DFResult<()> {
        let mapping = Arc::new(MemObjectIdMapping::new());
        let encoding = Arc::new(ObjectIdEncoding::new(
            Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
        ));

        let mut builder = PlainTermArrayElementBuilder::new(5);
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.com/a"));
        builder.append_blank_node(BlankNodeRef::new_unchecked("b1"));
        builder.append_literal(LiteralRef::new_typed_literal("hello", xsd::STRING));
        builder.append_literal(LiteralRef::new_language_tagged_literal_unchecked(
            "world", "en",
        ));
        builder.append_null();
        let plain_term_array = builder.finish();

        let object_id_array = encoding.encode_array(&plain_term_array)?;
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
        let mapping = Arc::new(MemObjectIdMapping::new());

        let mut builder = PlainTermArrayElementBuilder::new(5);
        let nn1 = NamedNodeRef::new_unchecked("http://example.com/a");
        let nn2 = NamedNodeRef::new_unchecked("http://example.com/b");

        // Add two identical terms and one different one
        builder.append_named_node(nn1);
        builder.append_named_node(nn2);
        builder.append_named_node(nn1);
        let plain_term_array = builder.finish();

        let object_id_array = mapping.encode_array(&plain_term_array)?;

        let id1 = object_id_array.value(0);
        let id2 = object_id_array.value(1);
        let id3 = object_id_array.value(2);

        assert_eq!(id1, id3);
        assert_ne!(id1, id2);

        // Now encode again, the IDs should be the same
        let mut builder2 = PlainTermArrayElementBuilder::new(2);
        builder2.append_named_node(nn2);
        builder2.append_named_node(nn1);
        let plain_term_array2 = builder2.finish();
        let object_id_array2 = mapping.encode_array(&plain_term_array2)?;

        let id4 = object_id_array2.value(0);
        let id5 = object_id_array2.value(1);

        assert_eq!(id2, id4);
        assert_eq!(id1, id5);

        Ok(())
    }

    #[test]
    fn test_try_get_object_id() -> DFResult<()> {
        let mapping = Arc::new(MemObjectIdMapping::new());

        let term1 = PlainTermScalar::from(TermRef::NamedNode(
            NamedNodeRef::new_unchecked("http://example.com/a"),
        ));
        let term2 =
            PlainTermScalar::from(TermRef::BlankNode(BlankNodeRef::new_unchecked("b1")));

        // Before encoding, should be None
        assert!(mapping.try_get_object_id(&term1)?.is_none());
        assert!(mapping.try_get_object_id(&term2)?.is_none());

        // Encode an array to populate the mapping
        let mut builder = PlainTermArrayElementBuilder::new(2);
        builder.append_named_node(NamedNodeRef::new_unchecked("http://example.com/a"));
        builder.append_blank_node(BlankNodeRef::new_unchecked("b1"));
        let plain_term_array = builder.finish();
        let object_id_array = mapping.encode_array(&plain_term_array)?;

        // After encoding, should be Some
        let object_id1 = mapping.try_get_object_id(&term1)?;
        assert!(object_id1.is_some());
        let object_id2 = mapping.try_get_object_id(&term2)?;
        assert!(object_id2.is_some());

        // Check if IDs match what's in the array
        assert_eq!(object_id1.unwrap().as_bytes(), object_id_array.value(0));
        assert_eq!(object_id2.unwrap().as_bytes(), object_id_array.value(1));

        // A term not in the mapping
        let term3 = PlainTermScalar::from(TermRef::NamedNode(
            NamedNodeRef::new_unchecked("http://example.com/c"),
        ));
        assert!(mapping.try_get_object_id(&term3)?.is_none());

        Ok(())
    }
}
