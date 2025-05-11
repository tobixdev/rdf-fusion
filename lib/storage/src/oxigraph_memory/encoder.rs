#![allow(clippy::unreadable_literal)]

use crate::oxigraph_memory::encoded_term::EncodedTerm;
use graphfusion_engine::error::{CorruptionError, StorageError};
use graphfusion_model::Term;
use graphfusion_model::TermRef;
use graphfusion_model::{GraphName, NamedNode, Quad, QuadRef, Subject};
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

pub trait Decoder {
    fn decode_term(&self, encoded: &EncodedTerm) -> Result<Term, StorageError>;

    fn decode_subject(&self, encoded: &EncodedTerm) -> Result<Subject, StorageError> {
        match self.decode_term(encoded)? {
            Term::NamedNode(named_node) => Ok(named_node.into()),
            Term::BlankNode(blank_node) => Ok(blank_node.into()),
            Term::Literal(_) => Err(CorruptionError::msg(
                "A literal has been found instead of a subject node",
            )
            .into()),
        }
    }

    fn decode_named_node(&self, encoded: &EncodedTerm) -> Result<NamedNode, StorageError> {
        match self.decode_term(encoded)? {
            Term::NamedNode(named_node) => Ok(named_node),
            Term::BlankNode(_) => Err(CorruptionError::msg(
                "A blank node has been found instead of a named node",
            )
            .into()),
            Term::Literal(_) => {
                Err(CorruptionError::msg("A literal has been found instead of a named node").into())
            }
        }
    }

    fn decode_quad(&self, encoded: &EncodedQuad) -> Result<Quad, StorageError> {
        Ok(Quad::new(
            self.decode_subject(&encoded.subject)?,
            self.decode_named_node(&encoded.predicate)?,
            self.decode_term(&encoded.object)?,
            if encoded.graph_name == EncodedTerm::DefaultGraph {
                GraphName::DefaultGraph
            } else {
                match self.decode_term(&encoded.graph_name)? {
                    Term::NamedNode(named_node) => named_node.into(),
                    Term::BlankNode(blank_node) => blank_node.into(),
                    Term::Literal(_) => {
                        return Err(
                            CorruptionError::msg("A literal is not a valid graph name").into()
                        )
                    }
                }
            },
        ))
    }
}

fn err_from_encoded_term(encoded: &EncodedTerm, term: &TermRef<'_>) -> CorruptionError {
    // TODO: eventually use a dedicated error enum value
    CorruptionError::msg(format!("Invalid term encoding {encoded:?} for {term}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(not(target_family = "wasm"))]
    use std::mem::{align_of, size_of};

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn test_size_and_alignment() {
        assert_eq!(size_of::<EncodedTerm>(), 40);
        assert_eq!(size_of::<EncodedQuad>(), 160);
        assert_eq!(align_of::<EncodedTerm>(), 8);
    }
}
