#![allow(clippy::unreadable_literal)]

use crate::oxigraph_memory::encoded_term::EncodedTerm;
use graphfusion_model::QuadRef;
use std::fmt::Debug;
use std::hash::Hash;

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct EncodedQuad {
    pub subject: EncodedTerm,
    pub predicate: EncodedTerm,
    pub object: EncodedTerm,
    pub graph_name: EncodedTerm,
}

impl EncodedQuad {
    pub fn new(
        subject: EncodedTerm,
        predicate: EncodedTerm,
        object: EncodedTerm,
        graph_name: EncodedTerm,
    ) -> Self {
        Self {
            subject,
            predicate,
            object,
            graph_name,
        }
    }
}

impl From<QuadRef<'_>> for EncodedQuad {
    fn from(quad: QuadRef<'_>) -> Self {
        Self {
            subject: quad.subject.into(),
            predicate: quad.predicate.into(),
            object: quad.object.into(),
            graph_name: quad.graph_name.into(),
        }
    }
}
