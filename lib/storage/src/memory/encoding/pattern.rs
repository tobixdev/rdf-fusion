use crate::memory::object_id::EncodedObjectId;

/// Represents an encoded version of a term pattern.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EncodedObjectIdPattern {
    /// Filter for the given object id.
    ObjectId(EncodedObjectId),
    /// Represents variables and blank node "variables".
    Variable,
}