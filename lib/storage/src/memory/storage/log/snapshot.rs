use crate::memory::storage::VersionNumber;
use crate::memory::storage::log::content::{LogChanges, MemLogContent};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Holds a snapshot of the log that can be read from.
///
/// The snapshot will only consider log entries with a smaller or equal version number.
#[derive(Debug, Clone)]
pub struct MemLogSnapshot {
    /// A lock-able version of the log content.
    ///
    /// Other transactions can concurrently modify the content as we do not keep a permanent read
    /// lock.
    content: Arc<RwLock<MemLogContent>>,
    /// The version number of the log.
    version_number: VersionNumber,
}

impl MemLogSnapshot {
    /// Create a new [MemLogSnapshot].
    pub fn new(
        content: Arc<RwLock<MemLogContent>>,
        version_number: VersionNumber,
    ) -> Self {
        Self {
            content,
            version_number,
        }
    }

    /// Returns the version number of the snapshot.
    pub fn version_number(&self) -> VersionNumber {
        self.version_number
    }

    /// Computes the changes up to the current version number. These changes should be used by the
    /// quad pattern stream to incorporate the changes in the log.
    pub async fn compute_changes(&self) -> Option<LogChanges> {
        self.content
            .read()
            .await
            .compute_changes(self.version_number)
    }
}
