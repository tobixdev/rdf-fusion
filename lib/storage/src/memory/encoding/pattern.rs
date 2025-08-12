use std::collections::HashSet;
use crate::memory::object_id::EncodedObjectId;

/// An encoded version of a triple pattern.
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct EncodedTriplePattern {
    pub subject: EncodedTermPattern,
    pub predicate: EncodedTermPattern,
    pub object: EncodedTermPattern,
}

/// Represents an encoded version of a term pattern.
#[derive(Debug, Clone)]
pub enum EncodedTermPattern {
    /// Filter for the given object id.
    ObjectId(EncodedObjectId),
    /// Represents variables and blank node "variables".
    Variable(String),
    /// Represents wildcards in the pattern. Optionally, a set of object ids can be provided that
    /// should be excluded from the wildcard.
    WildcardExcept(HashSet<EncodedObjectId>),
}

impl EncodedTermPattern {
    /// Returns true if the pattern is a variable.
    pub fn try_as_object_id(&self) -> Option<EncodedObjectId> {
        match self {
            EncodedTermPattern::ObjectId(oid) => Some(*oid),
            EncodedTermPattern::Variable(_) => None,
        }
    }
}

impl EncodedTermPattern {
    /// Returns true if the pattern is bound to an object id.
    pub fn is_bound(&self) -> bool {
        matches!(self, Self::ObjectId(_))
    }

    /// Returns true if the pattern is a variable.
    pub fn is_variable(&self) -> bool {
        matches!(self, Self::ObjectId(_))
    }
}
