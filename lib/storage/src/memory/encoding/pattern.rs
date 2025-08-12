use crate::memory::object_id::EncodedObjectId;

/// An encoded version of a triple pattern.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct EncodedTriplePattern {
    pub subject: EncodedTermPattern,
    pub predicate: EncodedTermPattern,
    pub object: EncodedTermPattern,
}

/// Represents an encoded version of a term pattern.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum EncodedTermPattern {
    /// Filter for the given object id.
    ObjectId(EncodedObjectId),
    /// Represents variables and blank node "variables".
    Variable(String),
}

impl EncodedTermPattern {
    /// Returns true if the pattern is a variable.
    pub fn try_as_object_id(&self) -> Option<EncodedObjectId> {
        match self {
            EncodedTermPattern::ObjectId(oid) => Some(*oid),
            EncodedTermPattern::Variable(_) => None,
        }
    }

    /// Returns true if the pattern is a variable.
    pub fn try_as_variable(&self) -> Option<&str> {
        match self {
            EncodedTermPattern::ObjectId(_) => None,
            EncodedTermPattern::Variable(var) => Some(var.as_str()),
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
