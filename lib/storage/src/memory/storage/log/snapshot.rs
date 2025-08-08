use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::log::VersionNumber;
use crate::memory::storage::log::content::{GraphLen, MemLogContent};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Holds a snapshot of the log that can be read from.
///
/// The snapshot will only consider log entries with a smaller or equal version number.
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

    /// Returns the size of all graphs at the current snapshot.
    pub async fn len(&self) -> GraphLen {
        self.content.read().await.len(self.version_number)
    }

    /// Checks whether the named graph `graph` exists in the current snapshot.
    pub async fn contains_named_graph(&self, graph: EncodedObjectId) -> bool {
        let len = self.len().await;
        len.graph.contains_key(&(Some(graph).into()))
    }
}
