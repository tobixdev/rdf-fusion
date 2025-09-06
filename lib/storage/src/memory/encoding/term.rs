use rdf_fusion_model::{BlankNodeRef, LiteralRef, NamedNodeRef, TermRef};
use std::sync::Arc;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub enum EncodedTerm {
    NamedNode(Arc<str>),
    BlankNode(Arc<str>),
    TypedLiteral(Arc<str>, Arc<str>),
    LangString(Arc<str>, Arc<str>),
}

impl EncodedTerm {
    pub fn first_str(&self) -> &Arc<str> {
        match self {
            EncodedTerm::NamedNode(nn) => nn,
            EncodedTerm::BlankNode(bnode) => bnode,
            EncodedTerm::TypedLiteral(value, _) => value,
            EncodedTerm::LangString(value, _) => value,
        }
    }

    pub fn second_str(&self) -> Option<&Arc<str>> {
        match self {
            EncodedTerm::NamedNode(_) => None,
            EncodedTerm::BlankNode(_) => None,
            EncodedTerm::TypedLiteral(_, data_type) => Some(data_type),
            EncodedTerm::LangString(_, lang) => Some(lang),
        }
    }
}

impl<'term> From<&'term EncodedTerm> for TermRef<'term> {
    fn from(value: &'term EncodedTerm) -> Self {
        match value {
            EncodedTerm::NamedNode(nn) => {
                TermRef::NamedNode(NamedNodeRef::new_unchecked(nn.as_ref()))
            }
            EncodedTerm::BlankNode(bnode) => {
                TermRef::BlankNode(BlankNodeRef::new_unchecked(bnode.as_ref()))
            }
            EncodedTerm::TypedLiteral(value, data_type) => {
                TermRef::Literal(LiteralRef::new_typed_literal(
                    value.as_ref(),
                    NamedNodeRef::new_unchecked(data_type.as_ref()),
                ))
            }
            EncodedTerm::LangString(value, lang) => {
                TermRef::Literal(LiteralRef::new_language_tagged_literal_unchecked(
                    value.as_ref(),
                    lang.as_ref(),
                ))
            }
        }
    }
}
