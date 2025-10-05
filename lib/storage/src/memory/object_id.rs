#![allow(clippy::unreadable_literal)]

use datafusion::parquet::data_type::AsBytes;
use rdf_fusion_model::ObjectId;
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use thiserror::Error;

const SIZE: u8 = 4;

/// The encoded object id represents an [ObjectId] in the storage layer.
///
/// # Default Graph
///
/// The default graph is represented by the [DEFAULT_GRAPH_ID]. Use [EncodedGraphObjectId] to
/// indicate that an id may represent the default graph. If the id is not a [EncodedGraphObjectId],
/// the system assumes that the id cannot represent the default graph and errors may be thrown
/// during decoding.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct EncodedObjectId(u32);

impl EncodedObjectId {
    pub const SIZE: u8 = 4;
    pub const SIZE_I32: i32 = SIZE as i32;
    pub const MIN: EncodedObjectId = EncodedObjectId(0);
    pub const MAX: EncodedObjectId = EncodedObjectId(u32::MAX);

    pub fn as_object_id(&self) -> ObjectId {
        ObjectId::from(self.0)
    }

    pub fn as_u32(&self) -> u32 {
        self.0
    }

    pub fn next(&self) -> Option<EncodedObjectId> {
        self.0.checked_add(1).map(EncodedObjectId)
    }

    pub fn previous(&self) -> Option<EncodedObjectId> {
        self.0.checked_sub(1).map(EncodedObjectId)
    }
}

impl From<u32> for EncodedObjectId {
    fn from(value: u32) -> Self {
        Self(value)
    }
}

impl Display for EncodedObjectId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Error)]
#[error("Invalid object ID.")]
pub struct InvalidObjectIdError;

impl TryFrom<&[u8]> for EncodedObjectId {
    type Error = InvalidObjectIdError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        TryInto::<[u8; 4]>::try_into(value.as_bytes())
            .map(u32::from_ne_bytes)
            .map(Self)
            .map_err(|_| InvalidObjectIdError)
    }
}

/// The id of the default graph.
pub const DEFAULT_GRAPH_ID: EncodedGraphObjectId =
    EncodedGraphObjectId(EncodedObjectId(0));

/// Wraps an [EncodedObjectId] to indicate that the id may represent the default graph.
///
/// The [DEFAULT_GRAPH_ID] is used for identifying the default graph.
#[derive(Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct EncodedGraphObjectId(pub EncodedObjectId);

impl EncodedGraphObjectId {
    /// Returns true if this id represents the default graph.
    pub fn is_default_graph(self) -> bool {
        self == DEFAULT_GRAPH_ID
    }

    /// Returns true if this id represents a named graph.
    pub fn is_named_graph(self) -> bool {
        !self.is_default_graph()
    }

    /// Returns the graph id as an [EncodedObjectId] if it is a named graph. Otherwise, returns
    /// [None].
    pub fn try_as_encoded_object_id(self) -> Option<EncodedObjectId> {
        if self.is_default_graph() {
            return None;
        }
        Some(self.0)
    }

    /// Returns the graph id as an [EncodedObjectId].
    ///
    /// # Panics
    ///
    /// This function will panic if this id represents the default graph. Due to performance
    /// reasons this validation will only be done in a Debug build.
    pub fn as_encoded_object_id(self) -> EncodedObjectId {
        self.try_as_encoded_object_id()
            .expect("Cannot convert default graph id to object id.")
    }
}

impl From<EncodedObjectId> for EncodedGraphObjectId {
    fn from(value: EncodedObjectId) -> Self {
        EncodedGraphObjectId(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_byte_slice_success() {
        let array: [u8; 4] = [0x56, 0x78, 0x9A, 0xBC];
        let id = EncodedObjectId::try_from(array.as_slice()).unwrap();
        assert_eq!(id.0.to_ne_bytes(), [0x56, 0x78, 0x9A, 0xBC]);
    }

    #[test]
    fn test_from_byte_slice_success_too_large() {
        let array: [u8; 3] = [0x56, 0x78, 0x9A];
        let result = EncodedObjectId::try_from(array.as_slice());
        assert!(result.is_err());
    }

    #[test]
    fn test_from_byte_slice_success_too_small() {
        let array: [u8; 5] = [0x56, 0x78, 0x9A, 0xBC, 0xDE];
        let result = EncodedObjectId::try_from(array.as_slice());
        assert!(result.is_err());
    }
}
