use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{
    IndexConfiguration, IndexScanInstruction, IndexedQuad,
};

/// Contains the logic for a single index level.
pub(super) trait IndexLevelImpl: Default + Sync {
    /// The type of the traversal state.
    type ScanState<'idx>: ScanState
    where
        Self: 'idx;

    /// Inserts the triple into the index.
    fn insert(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> bool;

    /// Deletes the triple from the index.
    fn remove(
        &mut self,
        configuration: &IndexConfiguration,
        triple: &IndexedQuad,
        cur_depth: usize,
    ) -> bool;

    /// The number of entries in the index part.
    fn num_triples(&self) -> usize;

    /// Creates a scan state for the given `index_scan_instructions`.
    fn create_scan_state(
        &self,
        configuration: &IndexConfiguration,
        index_scan_instructions: Vec<IndexScanInstruction>,
    ) -> Self::ScanState<'_>;
}

pub(super) trait ScanState: Send {
    /// Continues
    fn scan(
        self,
        configuration: &IndexConfiguration,
        collector: &mut ScanCollector,
    ) -> (usize, Option<Self>)
    where
        Self: Sized;
}
