use rdf_fusion_common::error::{CorruptionError, StorageError};
use std::ops::DerefMut;
use std::sync::Arc;
use tokio::sync::RwLock;

mod builder;
mod content;
mod snapshot;
mod state_root;
mod validation;
mod writer;

use crate::memory::storage::log::content::MemLogContent;
use crate::memory::storage::log::state_root::HistoricLogStateSource;
use crate::memory::storage::log::validation::validate_mem_log;
use crate::memory::storage::log::writer::MemLogWriter;
use crate::memory::storage::VersionNumber;
use crate::memory::MemObjectIdMapping;
pub use content::ClearTarget;
pub use content::LogChanges;
pub use snapshot::MemLogSnapshot;
pub use state_root::EmptyHistoricLogStateSource;

/// The [MemLog] keeps track of all the operations that have been performed on the store. As a
/// result, the store state can be completely reconstructed from the log.
pub struct MemLog {
    /// Holds the object id mapping
    object_id_mapping: Arc<MemObjectIdMapping>,
    /// Holds the log entries for each transaction.
    content: Arc<RwLock<MemLogContent>>,
    /// The historic state of this log.
    state_root: Box<dyn HistoricLogStateSource>,
}

impl MemLog {
    /// Creates a new [MemLog].
    pub fn new(
        object_id_mapping: Arc<MemObjectIdMapping>,
        state_root: Box<dyn HistoricLogStateSource>,
    ) -> Self {
        Self {
            object_id_mapping,
            content: Arc::new(RwLock::new(MemLogContent::new())),
            state_root,
        }
    }

    /// Creates a [MemLogSnapshot] of the [MemLog] for reading.
    pub async fn snapshot(&self) -> MemLogSnapshot {
        // Only keep the lock shortly for reading the version number to avoid
        let version_number = self.content.read().await.version_number();
        MemLogSnapshot::new(self.content.clone(), version_number)
    }

    /// Creates a new [MemLogWriter].
    ///
    /// Currently, we take a mutable reference to the log to ensure that only one writer is active
    /// at a time.
    pub async fn transaction<T>(
        &self,
        action: impl for<'a> Fn(&mut MemLogWriter<'a>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut content = self.content.write().await;

        let state_root = self
            .state_root
            .create_for_version(content.version_number())?;
        let mut writer = MemLogWriter::new(
            content.deref_mut(),
            &self.object_id_mapping,
            content.version_number().next(),
            state_root,
        );
        let result = action(&mut writer);

        match result {
            Ok(result) => {
                let log_entry = writer.into_log_entry()?;

                // Some action may not even create a log entry (e.g., only inserting duplicates).
                if let Some(log_entry) = log_entry {
                    content.deref_mut().append_log_entry(log_entry);
                }

                Ok(result)
            }
            Err(err) => Err(err),
        }
    }

    /// Clear all log entries with a version number <= `version_number`.
    pub async fn clear_log_until(&self, version_number: VersionNumber) {
        self.content.write().await.clear_log_until(version_number)
    }

    /// Validates the log.
    ///
    /// See [MemLogValidator] for details.
    pub async fn validate(&self) -> Result<(), StorageError> {
        let content = self.content.read().await;
        match validate_mem_log(&content) {
            Ok(_) => Ok(()),
            Err(errors) => {
                let mut error = String::from("Corruption(s) detected in log:\n");
                for corr_error in errors {
                    error.push_str(&format!("- {corr_error}\n"));
                }
                Err(StorageError::Corruption(CorruptionError::msg(error)))
            }
        }
    }
}

/// Defines how the logs should be managed.
pub enum LogRetentionPolicy {
    /// Removes all log entries after they have been applied to all indices.
    DeleteAfterIndexUpdate,
}

#[cfg(test)]
mod test {
    use crate::memory::object_id::EncodedObjectId;
    use crate::memory::storage::log::content::MemLogEntryAction;
    use crate::memory::storage::log::state_root::EmptyHistoricLogStateSource;
    use crate::memory::storage::log::MemLog;
    use crate::memory::MemObjectIdMapping;
    use rdf_fusion_model::{GraphName, NamedNode, Quad};
    use std::sync::Arc;

    #[tokio::test]
    async fn insert_and_then_load() {
        let mapping = Arc::new(MemObjectIdMapping::new());
        let log = MemLog::new(
            mapping.clone(),
            Box::new(EmptyHistoricLogStateSource::default()),
        );

        log.transaction(|w| {
            w.insert_quads(&[Quad::new(
                NamedNode::new_unchecked("www.example.com/s"),
                NamedNode::new_unchecked("www.example.com/p"),
                NamedNode::new_unchecked("www.example.com/o"),
                GraphName::default(),
            )])
        })
        .await
        .unwrap();

        assert_eq!(
            log.snapshot()
                .await
                .compute_changes()
                .await
                .unwrap()
                .inserted
                .iter()
                .filter(|q| q.graph_name.is_default_graph())
                .count(),
            1
        );

        let content = log.content.read().await;
        let MemLogEntryAction::Update(log) = &content.log_entries()[0].action else {
            panic!("Expected an update log entry");
        };
        let encoded_quad = log.insertions().into_iter().next().unwrap();

        assert_eq!(
            lookup_object_id(mapping.as_ref(), encoded_quad.subject).as_ref(),
            "www.example.com/s"
        );
        assert_eq!(
            lookup_object_id(mapping.as_ref(), encoded_quad.predicate).as_ref(),
            "www.example.com/p"
        );
        assert_eq!(
            lookup_object_id(mapping.as_ref(), encoded_quad.object).as_ref(),
            "www.example.com/o"
        );
    }

    #[tokio::test]
    async fn insert_remove_insert() {
        let mapping = Arc::new(MemObjectIdMapping::new());
        let log = MemLog::new(
            mapping.clone(),
            Box::new(EmptyHistoricLogStateSource::default()),
        );
        let quad = Quad::new(
            NamedNode::new_unchecked("www.example.com/s"),
            NamedNode::new_unchecked("www.example.com/p"),
            NamedNode::new_unchecked("www.example.com/o"),
            GraphName::default(),
        );

        log.transaction(|w| w.insert_quads(&[quad.clone()]))
            .await
            .unwrap();

        log.transaction(|w| w.delete(&[quad.as_ref()]))
            .await
            .unwrap();

        log.transaction(|w| w.insert_quads(&[quad.clone()]))
            .await
            .unwrap();

        let changes = log.snapshot().await.compute_changes().await.unwrap();
        assert_eq!(changes.inserted.len(), 1);
        assert_eq!(changes.deleted.len(), 0);
    }

    fn lookup_object_id(
        mapping: &MemObjectIdMapping,
        object_id: EncodedObjectId,
    ) -> Arc<str> {
        mapping
            .try_get_encoded_term_from_object_id(object_id)
            .unwrap()
            .first_str()
            .clone()
    }
}
