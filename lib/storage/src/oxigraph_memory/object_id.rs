#![allow(clippy::unreadable_literal)]

use rdf_fusion_common::ObjectId;
use std::fmt::Debug;
use std::hash::Hash;
use thiserror::Error;

const SIZE: u8 = 6;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Copy, PartialOrd, Ord)]
pub struct EncodedObjectId([u8; SIZE as usize]);

impl EncodedObjectId {
    pub const OBJECT_ID_SIZE: u8 = SIZE;
    pub const OBJECT_ID_SIZE_I32: i32 = SIZE as i32;
}

impl From<EncodedObjectId> for ObjectId {
    fn from(value: EncodedObjectId) -> Self {
        ObjectId::from(&value.0)
    }
}

#[derive(Debug, Error)]
#[error("Invalid object ID.")]
pub struct InvalidObjectIdError;

impl TryFrom<ObjectId> for EncodedObjectId {
    type Error = InvalidObjectIdError;

    fn try_from(value: ObjectId) -> Result<Self, Self::Error> {
        TryInto::<[u8; OBJECT_ID_SIZE]>::try_into(value.as_bytes())
            .map(Self)
            .map_err(|_| InvalidObjectIdError)
    }
}

#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub struct ObjectIdQuad {
    pub graph_name: Option<ObjectId>,
    pub subject: ObjectId,
    pub predicate: ObjectId,
    pub object: ObjectId,
}
