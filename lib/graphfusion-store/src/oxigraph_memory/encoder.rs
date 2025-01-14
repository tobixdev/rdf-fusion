#![allow(clippy::unreadable_literal)]

use crate::oxigraph_memory::encoded_term::EncodedTerm;
use crate::oxigraph_memory::hash::StrHash;
use graphfusion_engine::error::{CorruptionError, StorageError};
use oxrdf::TermRef;
use oxrdf::TripleRef;
use oxrdf::{BlankNode, GraphName, Literal, NamedNode, Quad, QuadRef, Subject, Term, Triple};
use std::fmt::Debug;
use std::hash::Hash;
use std::str;

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct EncodedTriple {
    pub subject: EncodedTerm,
    pub predicate: EncodedTerm,
    pub object: EncodedTerm,
}

impl EncodedTriple {
    pub fn new(subject: EncodedTerm, predicate: EncodedTerm, object: EncodedTerm) -> Self {
        Self {
            subject,
            predicate,
            object,
        }
    }
}

impl From<TripleRef<'_>> for EncodedTriple {
    fn from(triple: TripleRef<'_>) -> Self {
        Self {
            subject: triple.subject.into(),
            predicate: triple.predicate.into(),
            object: triple.object.into(),
        }
    }
}

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

pub trait StrLookup {
    fn get_str(&self, key: &StrHash) -> Result<Option<String>, StorageError>;
}

pub fn insert_term<F: FnMut(&StrHash, &str) -> Result<(), StorageError>>(
    term: TermRef<'_>,
    encoded: &EncodedTerm,
    insert_str: &mut F,
) -> Result<(), StorageError> {
    match term {
        TermRef::NamedNode(node) => {
            if let EncodedTerm::NamedNode { iri_id } = encoded {
                insert_str(iri_id, node.as_str())
            } else {
                Err(err_from_encoded_term(encoded, &term).into())
            }
        }
        TermRef::BlankNode(node) => match encoded {
            EncodedTerm::BigBlankNode { id_id } => insert_str(id_id, node.as_str()),
            EncodedTerm::SmallBlankNode(..) | EncodedTerm::NumericalBlankNode { .. } => Ok(()),
            _ => Err(err_from_encoded_term(encoded, &term).into()),
        },
        TermRef::Literal(literal) => match encoded {
            EncodedTerm::BigStringLiteral { value_id }
            | EncodedTerm::BigSmallLangStringLiteral { value_id, .. } => {
                insert_str(value_id, literal.value())
            }
            EncodedTerm::SmallBigLangStringLiteral { language_id, .. } => {
                if let Some(language) = literal.language() {
                    insert_str(language_id, language)
                } else {
                    Err(err_from_encoded_term(encoded, &term).into())
                }
            }
            EncodedTerm::BigBigLangStringLiteral {
                value_id,
                language_id,
            } => {
                insert_str(value_id, literal.value())?;
                if let Some(language) = literal.language() {
                    insert_str(language_id, language)
                } else {
                    Err(err_from_encoded_term(encoded, &term).into())
                }
            }
            EncodedTerm::SmallTypedLiteral { datatype_id, .. } => {
                insert_str(datatype_id, literal.datatype().as_str())
            }
            EncodedTerm::BigTypedLiteral {
                value_id,
                datatype_id,
            } => {
                insert_str(value_id, literal.value())?;
                insert_str(datatype_id, literal.datatype().as_str())
            }
            EncodedTerm::SmallStringLiteral(..)
            | EncodedTerm::SmallSmallLangStringLiteral { .. }
            | EncodedTerm::BooleanLiteral(..)
            | EncodedTerm::FloatLiteral(..)
            | EncodedTerm::DoubleLiteral(..)
            | EncodedTerm::IntegerLiteral(..)
            | EncodedTerm::DecimalLiteral(..)
            | EncodedTerm::DateTimeLiteral(..)
            | EncodedTerm::TimeLiteral(..)
            | EncodedTerm::DateLiteral(..)
            | EncodedTerm::GYearMonthLiteral(..)
            | EncodedTerm::GYearLiteral(..)
            | EncodedTerm::GMonthDayLiteral(..)
            | EncodedTerm::GDayLiteral(..)
            | EncodedTerm::GMonthLiteral(..)
            | EncodedTerm::DurationLiteral(..)
            | EncodedTerm::YearMonthDurationLiteral(..)
            | EncodedTerm::DayTimeDurationLiteral(..) => Ok(()),
            _ => Err(err_from_encoded_term(encoded, &term).into()),
        },
        TermRef::Triple(triple) => {
            if let EncodedTerm::Triple(encoded) = encoded {
                insert_term(triple.subject.as_ref().into(), &encoded.subject, insert_str)?;
                insert_term(
                    triple.predicate.as_ref().into(),
                    &encoded.predicate,
                    insert_str,
                )?;
                insert_term(triple.object.as_ref(), &encoded.object, insert_str)
            } else {
                Err(err_from_encoded_term(encoded, &term).into())
            }
        }
    }
}

pub fn parse_boolean_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::BooleanLiteral).ok()
}

pub fn parse_float_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::FloatLiteral).ok()
}

pub fn parse_double_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::DoubleLiteral).ok()
}

pub fn parse_integer_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::IntegerLiteral).ok()
}

pub fn parse_decimal_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::DecimalLiteral).ok()
}

pub fn parse_date_time_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::DateTimeLiteral).ok()
}

pub fn parse_time_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::TimeLiteral).ok()
}

pub fn parse_date_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::DateLiteral).ok()
}

pub fn parse_g_year_month_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::GYearMonthLiteral).ok()
}

pub fn parse_g_year_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::GYearLiteral).ok()
}

pub fn parse_g_month_day_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::GMonthDayLiteral).ok()
}

pub fn parse_g_day_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::GDayLiteral).ok()
}

pub fn parse_g_month_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::GMonthLiteral).ok()
}

pub fn parse_duration_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::DurationLiteral).ok()
}

pub fn parse_year_month_duration_str(value: &str) -> Option<EncodedTerm> {
    value
        .parse()
        .map(EncodedTerm::YearMonthDurationLiteral)
        .ok()
}

pub fn parse_day_time_duration_str(value: &str) -> Option<EncodedTerm> {
    value.parse().map(EncodedTerm::DayTimeDurationLiteral).ok()
}

pub trait Decoder: StrLookup {
    fn decode_term(&self, encoded: &EncodedTerm) -> Result<Term, StorageError>;

    fn decode_subject(&self, encoded: &EncodedTerm) -> Result<Subject, StorageError> {
        match self.decode_term(encoded)? {
            Term::NamedNode(named_node) => Ok(named_node.into()),
            Term::BlankNode(blank_node) => Ok(blank_node.into()),
            Term::Literal(_) => Err(CorruptionError::msg(
                "A literal has been found instead of a subject node",
            )
            .into()),
            Term::Triple(triple) => Ok(Subject::Triple(triple)),
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
            Term::Triple(_) => {
                Err(CorruptionError::msg("A triple has been found instead of a named node").into())
            }
        }
    }

    fn decode_triple(&self, encoded: &EncodedTriple) -> Result<Triple, StorageError> {
        Ok(Triple::new(
            self.decode_subject(&encoded.subject)?,
            self.decode_named_node(&encoded.predicate)?,
            self.decode_term(&encoded.object)?,
        ))
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
                    Term::Triple(_) => {
                        return Err(
                            CorruptionError::msg("A triple is not a valid graph name").into()
                        )
                    }
                }
            },
        ))
    }
}

impl<S: StrLookup> Decoder for S {
    fn decode_term(&self, encoded: &EncodedTerm) -> Result<Term, StorageError> {
        match encoded {
            EncodedTerm::DefaultGraph => {
                Err(CorruptionError::msg("The default graph tag is not a valid term").into())
            }
            EncodedTerm::NamedNode { iri_id } => {
                Ok(NamedNode::new_unchecked(get_required_str(self, iri_id)?).into())
            }
            EncodedTerm::NumericalBlankNode { id } => {
                Ok(BlankNode::new_from_unique_id(u128::from_be_bytes(*id)).into())
            }
            EncodedTerm::SmallBlankNode(id) => Ok(BlankNode::new_unchecked(id.as_str()).into()),
            EncodedTerm::BigBlankNode { id_id } => {
                Ok(BlankNode::new_unchecked(get_required_str(self, id_id)?).into())
            }
            EncodedTerm::SmallStringLiteral(value) => {
                Ok(Literal::new_simple_literal(*value).into())
            }
            EncodedTerm::BigStringLiteral { value_id } => {
                Ok(Literal::new_simple_literal(get_required_str(self, value_id)?).into())
            }
            EncodedTerm::SmallSmallLangStringLiteral { value, language } => {
                Ok(Literal::new_language_tagged_literal_unchecked(*value, *language).into())
            }
            EncodedTerm::SmallBigLangStringLiteral { value, language_id } => {
                Ok(Literal::new_language_tagged_literal_unchecked(
                    *value,
                    get_required_str(self, language_id)?,
                )
                .into())
            }
            EncodedTerm::BigSmallLangStringLiteral { value_id, language } => {
                Ok(Literal::new_language_tagged_literal_unchecked(
                    get_required_str(self, value_id)?,
                    *language,
                )
                .into())
            }
            EncodedTerm::BigBigLangStringLiteral {
                value_id,
                language_id,
            } => Ok(Literal::new_language_tagged_literal_unchecked(
                get_required_str(self, value_id)?,
                get_required_str(self, language_id)?,
            )
            .into()),
            EncodedTerm::SmallTypedLiteral { value, datatype_id } => {
                Ok(Literal::new_typed_literal(
                    *value,
                    NamedNode::new_unchecked(get_required_str(self, datatype_id)?),
                )
                .into())
            }
            EncodedTerm::BigTypedLiteral {
                value_id,
                datatype_id,
            } => Ok(Literal::new_typed_literal(
                get_required_str(self, value_id)?,
                NamedNode::new_unchecked(get_required_str(self, datatype_id)?),
            )
            .into()),
            EncodedTerm::BooleanLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::FloatLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::DoubleLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::IntegerLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::DecimalLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::DateTimeLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::DateLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::TimeLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::GYearMonthLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::GYearLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::GMonthDayLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::GDayLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::GMonthLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::DurationLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::YearMonthDurationLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::DayTimeDurationLiteral(value) => Ok(Literal::from(*value).into()),
            EncodedTerm::Triple(triple) => Ok(self.decode_triple(triple)?.into()),
        }
    }
}

fn get_required_str<L: StrLookup>(lookup: &L, id: &StrHash) -> Result<String, StorageError> {
    Ok(lookup.get_str(id)?.ok_or_else(|| {
        CorruptionError::new(format!(
            "Not able to find the string with id {id:?} in the string store"
        ))
    })?)
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
