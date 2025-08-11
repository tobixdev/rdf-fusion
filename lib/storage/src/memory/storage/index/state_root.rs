use crate::memory::storage::index::data::IndexContent;
use crate::memory::storage::log::LogStateRoot;
use rdf_fusion_model::QuadRef;
use tokio::sync::OwnedRwLockReadGuard;

/// Provides the state root of the log via an index.
pub struct IndexStateRoot {
    /// The index. We must hold the read lock on this index for the entire lifetime of this struct,
    /// as we must ensure that exactly the version before the first log entry is reflected.
    index: OwnedRwLockReadGuard<IndexContent>,
}

impl LogStateRoot for IndexStateRoot {
    fn filter_existing_quads(
        &self,
        quads: impl IntoIterator<Item = QuadRef<'_>>,
    ) -> impl Iterator<Item = QuadRef<'_>> {
        todo!()
    }

    fn filter_non_existing_quads(
        &self,
        quads: impl IntoIterator<Item = QuadRef<'_>>,
    ) -> impl Iterator<Item = QuadRef<'_>> {
        todo!()
    }
}
