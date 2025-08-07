use crate::memory::storage::log::MemLogSnapshot;

/// Provides a snapshot view on the storage. Other transactions can read and write to the storage
/// without changing the view of the snapshot.
pub struct MemQuadStorageSnapshot {
    /// A snapshot of the log. This has a sequence of all changes to the system.
    log_snapshot: MemLogSnapshot,
}

impl MemQuadStorageSnapshot {
    /// Create a new [MemQuadStorageSnapshot].
    pub fn new(log_snapshot: MemLogSnapshot) -> Self {
        Self { log_snapshot }
    }

    /// Returns the number of quads in the storage.
    pub async fn len(&self) -> usize {
        let changes = self.log_snapshot.count_changes().await;
        changes.insertions - changes.deletions
    }
}
