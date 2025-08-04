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
        id_bytes.copy_from_slice(&bytes[..SIZE as usize]);

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
