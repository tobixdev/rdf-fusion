#![allow(clippy::unreadable_literal)]

use datafusion::parquet::data_type::AsBytes;
use rdf_fusion_common::ObjectId;
use std::fmt::Debug;
use std::hash::Hash;
use thiserror::Error;

const SIZE: u8 = 6;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct EncodedObjectId([u8; SIZE as usize]);

impl EncodedObjectId {
    pub const SIZE: u8 = SIZE;
    pub const SIZE_I32: i32 = SIZE as i32;
}

impl AsRef<[u8]> for EncodedObjectId {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl From<EncodedObjectId> for ObjectId {
    fn from(value: EncodedObjectId) -> Self {
        ObjectId::from(value.0.as_bytes())
    }
}

#[derive(Debug, Error)]
#[error("Invalid object ID.")]
pub struct InvalidObjectIdError;

impl TryFrom<ObjectId> for EncodedObjectId {
    type Error = InvalidObjectIdError;

    fn try_from(value: ObjectId) -> Result<Self, Self::Error> {
        TryFrom::try_from(value.as_bytes())
    }
}

impl TryFrom<&[u8]> for EncodedObjectId {
    type Error = InvalidObjectIdError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        TryInto::<[u8; EncodedObjectId::SIZE as usize]>::try_into(value.as_bytes())
            .map(Self)
            .map_err(|_| InvalidObjectIdError)
    }
}

impl TryFrom<u64> for EncodedObjectId {
    type Error = InvalidObjectIdError;

    fn try_from(value: u64) -> Result<Self, Self::Error> {
        // Check if the value can fit into the specified number of bytes
        let max_value = (1u64 << (SIZE * 8)) - 1;
        if value > max_value {
            return Err(InvalidObjectIdError);
        }

        let bytes = value.to_be_bytes();
        let mut id_bytes = [0u8; SIZE as usize];
        id_bytes.copy_from_slice(&bytes[(8 - SIZE as usize)..]);

        Ok(EncodedObjectId(id_bytes))
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct ObjectIdQuad {
    pub graph_name: Option<ObjectId>,
    pub subject: ObjectId,
    pub predicate: ObjectId,
    pub object: ObjectId,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_u64_success() {
        // Test with 0
        let id = EncodedObjectId::try_from(0).unwrap();
        assert_eq!(id.0, [0, 0, 0, 0, 0, 0]);

        // Test with 1
        let id = EncodedObjectId::try_from(1).unwrap();
        assert_eq!(id.0, [0, 0, 0, 0, 0, 1]);

        // Test with a value that uses more bytes
        let val = u64::from_be_bytes([0, 0, 0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC]);
        let id = EncodedObjectId::try_from(val).unwrap();
        assert_eq!(id.0, [0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC]);

        // Test with max value
        let max_value = (1u64 << 48) - 1;
        let id = EncodedObjectId::try_from(max_value).unwrap();
        let expected_bytes = max_value.to_be_bytes();
        assert_eq!(id.0, &expected_bytes[2..]);
    }

    #[test]
    fn test_try_from_u64_failure_too_large() {
        let too_large_value = 1u64 << 48;
        let result = EncodedObjectId::try_from(too_large_value);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), InvalidObjectIdError));
    }
}