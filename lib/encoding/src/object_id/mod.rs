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
pub struct ObjectIdCreationError;

impl TryFrom<i32> for ObjectIdSize {
    type Error = ObjectIdCreationError;

    fn try_from(value: i32) -> Result<Self, Self::Error> {
        if value > 0 {
            Ok(Self(value))
        } else {
            Err(ObjectIdCreationError)
        }
    }
}

impl From<ObjectIdSize> for i32 {
    fn from(value: ObjectIdSize) -> Self {
        value.0
    }
}

impl TryFrom<usize> for ObjectIdSize {
    type Error = ObjectIdCreationError;

    fn try_from(value: usize) -> Result<Self, Self::Error> {
        i32::try_from(value)
            .map(Self)
            .map_err(|_| ObjectIdCreationError)
    }
}

impl From<ObjectIdSize> for usize {
    fn from(value: ObjectIdSize) -> Self {
        value.0 as usize // This works because non-negativity is checked in the constructor
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
///
/// # Default Graph
///
/// The default graph is always encoded as the object id with all bytes set to zero. The number of
/// zero bytes depends on the [`ObjectIdSize`].
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ObjectId(Box<[u8]>);

impl ObjectId {
    /// Creates a new [`ObjectId`].
    ///
    /// # Errors
    ///
    /// Returns an error if the slice length does not fit in an `i32`.
    pub fn try_new(bytes: impl Into<Box<[u8]>>) -> Result<Self, ObjectIdCreationError> {
        let bytes = bytes.into();
        i32::try_from(bytes.len()).map_err(|_| ObjectIdCreationError)?;
        Ok(Self(bytes))
    }

    /// Creates a new [`ObjectId`] for the default graph with the given `size`.
    pub fn new_default_graph(size: ObjectIdSize) -> Self {
        Self(vec![0; size.0 as usize].into_boxed_slice())
    }

    /// Returns true if the object id is the default graph.
    pub fn is_default_graph(&self) -> bool {
        self.0.iter().all(|b| *b == 0)
    }

    /// Creates a new [`ObjectId`].
    pub fn try_new_from_array(array: &FixedSizeBinaryArray, index: usize) -> Self {
        let len = array.value_length() as usize;
        match array.is_valid(index) {
            true => Self(array.value(index).into()),
            false => Self(vec![0; len].into_boxed_slice()),
        }
    }

    /// Returns the length of the object id in bytes.
    pub fn size(&self) -> i32 {
        self.0.len() as i32 // Conversion checked in Self::try_new
    }

    /// Returns a reference to the underlying bytes.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}
