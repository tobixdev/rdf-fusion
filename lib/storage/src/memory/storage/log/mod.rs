use rdf_fusion_common::error::StorageError;
use std::ops::DerefMut;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::RwLock;

mod builder;
mod content;
mod snapshot;
mod writer;

use crate::memory::storage::log::content::MemLogContent;
pub use crate::memory::storage::log::writer::MemLogWriter;
use crate::memory::MemObjectIdMapping;
pub use snapshot::MemLogSnapshot;

/// The version number of the [MemLog].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VersionNumber(usize);

impl VersionNumber {
    /// Creates the next version number.
    pub fn increment(&self) -> Self {
        Self(self.0 + 1)
    }
}

/// The [MemLog] keeps track of all the operations that have been performed on the store. As a
/// result, the store state can be completely reconstructed from the log.
pub struct MemLog {
    /// Holds the log entries for each transaction.
    content: Arc<RwLock<MemLogContent>>,
    /// The current version number.
    version_number: AtomicUsize,
}

impl MemLog {
    /// Creates a new [MemLog].
    pub fn new() -> Self {
        Self {
            content: Arc::new(RwLock::new(MemLogContent::new())),
            version_number: AtomicUsize::new(0),
        }
    }

    /// Creates a [MemLogSnapshot] of the [MemLog] for reading.
    pub fn snapshot(&self) -> MemLogSnapshot {
        let version_number = self.version_number.load(Ordering::Relaxed);
        MemLogSnapshot::new(self.content.clone(), VersionNumber(version_number))
    }

    /// Creates a new [MemLogWriter].
    ///
    /// Currently, we take a mutable reference to the log to ensure that only one writer is active
    /// at a time.
    pub async fn transaction<T>(
        &self,
        object_id_mapping: &MemObjectIdMapping,
        action: impl for<'a> Fn(&mut MemLogWriter<'a>) -> Result<T, StorageError>,
    ) -> Result<T, StorageError> {
        let mut content = self.content.write().await;
        let version_number = self.version_number.load(Ordering::Relaxed);
        let mut writer = MemLogWriter::new(
            content.deref_mut(),
            object_id_mapping,
            VersionNumber(version_number),
        );
        let result = action(&mut writer);

        match result {
            Ok(result) => {
                let log_entry = writer.into_log_entry()?;

                // Some action may not even create a log entry (e.g., only inserting duplicates).
                if let Some(log_entry) = log_entry {
                    content.deref_mut().append_log_entry(log_entry);
                    self.version_number.fetch_add(1, Ordering::Release);
                }

                Ok(result)
            }
            Err(err) => Err(err),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::memory::object_id::EncodedObjectId;
    use crate::memory::storage::log::MemLog;
    use crate::memory::MemObjectIdMapping;
    use rdf_fusion_model::{GraphName, NamedNode, Quad};
    use std::sync::Arc;

    #[tokio::test]
    async fn insert_and_then_load() {
        let mapping = MemObjectIdMapping::new();
        let log = MemLog::new();

        log.transaction(&mapping, |w| {
            w.insert_quads(&[Quad::new(
                NamedNode::new_unchecked("www.example.com/s"),
                NamedNode::new_unchecked("www.example.com/p"),
                NamedNode::new_unchecked("www.example.com/o"),
                GraphName::default(),
            )])
        })
        .await
        .unwrap();

        assert_eq!(log.snapshot().count_changes().await.insertions, 1);

        let content = log.content.read().await;
        let log = &content.log_arrays()[0];
        let encoded_quad = log.insertions().into_iter().next().unwrap();

        assert_eq!(
            lookup_object_id(&mapping, encoded_quad.subject).as_ref(),
            "www.example.com/s"
        );
        assert_eq!(
            lookup_object_id(&mapping, encoded_quad.predicate).as_ref(),
            "www.example.com/p"
        );
        assert_eq!(
            lookup_object_id(&mapping, encoded_quad.object).as_ref(),
            "www.example.com/o"
        );
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
