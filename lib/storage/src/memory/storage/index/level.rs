use crate::memory::storage::index::scan_collector::ScanCollector;
use crate::memory::storage::index::{IndexConfiguration, IndexedQuad};

/// Contains the logic for a single index level.
pub(super) trait IndexLevelImpl: Default {
    /// The type of the traversal state.
    type ScanState: Clone;

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

    /// Executes the action for scanning the index at `cur_depth`.
    fn scan(
        &self,
        configuration: &IndexConfiguration,
        state: Self::ScanState,
        collector: &mut ScanCollector,
    ) -> (usize, Option<Self::ScanState>);
}
