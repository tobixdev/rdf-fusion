use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::log::content::{MemLogContent, MemLogEntryAction};
use crate::memory::storage::log::VersionNumber;
use std::collections::HashMap;
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

/// Counts the number of insertions and deletions in a log snapshot between two version numbers.
pub struct ChangeCount {
    /// The number of insertions.
    pub insertions: usize,
    /// The number of deletions.
    pub deletions: usize,
}

/// Counts the number of insertions and deletions for each graph in a log snapshot.
pub struct ChangeCounts {
    /// Mapping from graph to the change.
    pub graph: HashMap<EncodedObjectId, ChangeCount>,
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

    /// Counts the changes in the log up until the version number of the snapshot.
    pub async fn count_changes(&self) -> ChangeCount {
        let content = self.content.read().await;

        let mut insertions = 0;
        let mut deletions = 0;

        for entry in content
            .log_entries()
            .iter()
            .take_while(|e| e.version_number <= self.version_number)
        {
            match &entry.action {
                MemLogEntryAction::Update(update) => {
                    insertions += update.insertions().len();
                    deletions += update.deletions().len();
                }
                _ => todo!(),
            }
        }

        ChangeCount {
            insertions,
            deletions,
        }
    }

    /// Checks whether the named graph `graph` exists in the current snapshot.
    pub async fn contains_named_graph(&self, graph: EncodedObjectId) -> bool {
        self.content
            .read()
            .await
            .contains_named_graph(graph, self.version_number)
    }
}
