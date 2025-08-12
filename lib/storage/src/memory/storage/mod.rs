mod index;
mod log;
mod mem_storage;
mod pattern_exec;
mod snapshot;
mod stream;

use crate::memory::encoding::EncodedTermPattern;
use crate::memory::MemObjectIdMapping;
pub use mem_storage::MemQuadStorage;
pub use pattern_exec::MemQuadPatternExec;
use rdf_fusion_common::BlankNodeMatchingMode;
use rdf_fusion_encoding::object_id::UnknownObjectIdError;
use rdf_fusion_model::TermPattern;
pub use snapshot::MemQuadStorageSnapshot;
use std::fmt::{Display, Formatter};

/// The version number of the storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
