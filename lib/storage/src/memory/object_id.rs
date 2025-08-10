#![allow(clippy::unreadable_literal)]

use datafusion::parquet::data_type::AsBytes;
use rdf_fusion_common::ObjectId;
use std::fmt::Debug;
use std::hash::Hash;
use thiserror::Error;

const SIZE: u8 = 4;

/// The encoded object id represents an [ObjectId] in the storage layer.
///
/// # Why a Byte Array?
///
/// While the regular [ObjectId] is fixed to `u32` ids, this id is composed of a statically sized
/// byte array. The idea is to support arbitrary byte-arrays for object ids in the future. These are
/// the remains of one such experiment and should be furthered in the future.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct EncodedObjectId([u8; SIZE as usize]);

impl EncodedObjectId {
    pub const SIZE: u8 = SIZE;
    pub const SIZE_I32: i32 = SIZE as i32;

    pub fn as_object_id(&self) -> ObjectId {
        ObjectId::from(u32::from_ne_bytes(self.0))
    }

    pub fn as_u32(&self) -> u32 {
        u32::from_ne_bytes(self.0)
    }
}

impl From<u32> for EncodedObjectId {
    fn from(value: u32) -> Self {
        Self(value.to_ne_bytes())
    }
}

impl AsRef<[u8]> for EncodedObjectId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Debug, Error)]
#[error("Invalid object ID.")]
pub struct InvalidObjectIdError;

impl TryFrom<&[u8]> for EncodedObjectId {
    type Error = InvalidObjectIdError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        TryInto::<[u8; EncodedObjectId::SIZE as usize]>::try_into(value.as_bytes())
            .map(Self)
            .map_err(|_| InvalidObjectIdError)
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Copy, Hash)]
pub struct GraphEncodedObjectId(pub Option<EncodedObjectId>);

impl GraphEncodedObjectId {
    pub fn is_default_graph(&self) -> bool {
        self.0.is_none()
    }
}

impl From<Option<EncodedObjectId>> for GraphEncodedObjectId {
    fn from(value: Option<EncodedObjectId>) -> Self {
        GraphEncodedObjectId(value)
    }
}

impl From<GraphEncodedObjectId> for Option<EncodedObjectId> {
    fn from(value: GraphEncodedObjectId) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_byte_slice_success() {
        let array: [u8; 4] = [0x56, 0x78, 0x9A, 0xBC];
        let id = EncodedObjectId::try_from(array.as_slice()).unwrap();
        assert_eq!(id.0, [0x56, 0x78, 0x9A, 0xBC]);
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
