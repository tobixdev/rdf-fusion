use crate::memory::object_id::EncodedObjectId;

/// Represents an encoded version of a term pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncodedObjectIdPattern {
    /// Filter for the given object id.
    ObjectId(EncodedObjectId),
    /// Represents variables and blank node "variables".
    Variable,
}

impl EncodedObjectIdPattern {
    /// Returns true if the pattern is bound to an object id.
    pub fn is_bound(&self) -> bool {
        matches!(self, Self::ObjectId(_))
    }

    /// Returns true if the pattern is a variable.
    pub fn is_variable(&self) -> bool {
        matches!(self, Self::ObjectId(_))
    }
}
