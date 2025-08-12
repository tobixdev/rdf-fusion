mod index;
mod mem_storage;
mod pattern_exec;
mod pattern_stream;
mod snapshot;

pub use mem_storage::MemQuadStorage;
pub use pattern_exec::MemQuadPatternExec;
pub use pattern_stream::MemQuadPatternStream;
pub use snapshot::MemQuadStorageSnapshot;
use std::fmt::{Display, Formatter};

/// The version number of the storage.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionNumber(u64);

impl Display for VersionNumber {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl VersionNumber {
    /// Creates the next version number.
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }
}
