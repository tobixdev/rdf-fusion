mod array;
mod encoding;
mod mapping;
mod scalar;

pub use array::*;
use datafusion::arrow::array::{Array, FixedSizeBinaryArray};
pub use encoding::*;
pub use mapping::*;
pub use scalar::*;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// The size of an object id in bytes.
///
/// An `i32` is used for the size as this is used by Arrow. The length will always be greater than
/// zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectIdSize(i32);

#[derive(Error, Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[error("Invalid object id size.")]
pub struct ObjectIdTooLargeError;

impl TryFrom<i32> for ObjectIdSize {
    type Error = ObjectIdTooLargeError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(ObjectIdTooLargeError)
        }
    }
}

impl From<ObjectIdSize> for i32 {
    fn from(value: ObjectIdSize) -> Self {
        value.0
    }
}

impl TryFrom<usize> for ObjectIdSize {
    type Error = ObjectIdTooLargeError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        i32::try_from(value)
            .map(Self)
            .map_err(|_| ObjectIdTooLargeError)
    }
}

impl Display for ObjectIdSize {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} Bytes", self.0)
    }
}

/// An object id that is not yet related to any [`ObjectIdEncoding`]. For an object id that is
/// related to a specific encoding see [`ObjectIdScalar`].
///
/// This struct guarantees that the slice length fits into a non-negative `i32`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId {
    len: i32,
    slice: Box<[u8]>,
}

impl ObjectId {
    /// Creates a new [`ObjectId`].
    pub fn try_new(bytes: impl Into<Box<[u8]>>) -> Result<Self, ObjectIdTooLargeError> {
        let bytes = bytes.into();
        let len = i32::try_from(bytes.len()).map_err(|_| ObjectIdTooLargeError)?;
        Ok(Self { len, slice: bytes })
    }

    /// Creates a new [`ObjectId`].
    pub fn try_new_from_array(
        array: &FixedSizeBinaryArray,
        index: usize,
    ) -> Option<Self> {
        if !array.is_valid(index) {
            return None;
        }
        let value = Vec::from(array.value(index));
        Some(ObjectId {
            len: array.value_length(),
            slice: value.into_boxed_slice(),
        })
    }

    /// Returns the length of the object id in bytes.
    pub fn size(&self) -> i32 {
        self.len
    }

    /// Returns a reference to the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.slice
    }
}
