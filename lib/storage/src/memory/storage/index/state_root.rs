use crate::memory::storage::index::data::IndexContent;
use tokio::sync::OwnedRwLockReadGuard;

/// Provides the state root of the log via an index.
pub struct IndexStateRoot {
    /// The index. We must hold the read lock on this index for the entire lifetime of this struct,
    /// as we must ensure that exactly the version before the first log entry is reflected.
    index: OwnedRwLockReadGuard<IndexContent>,
}

impl IndexStateRoot {
    /// Creates a new [IndexStateRoot] with the given index.
    pub fn new(index: OwnedRwLockReadGuard<IndexContent>) -> Self {
        Self { index }
    }
}
