use crate::memory::storage::VersionNumber;
use rdf_fusion_model::QuadRef;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum HistoricLogStateCreationError {
    #[error("The requested version number is not available")]
    UnavailableVersionNumber,
}

/// Provides the initial state for writing operation on the log.
///
/// Depending on the log retention policy, the entire store state cannot be contructed from the log.
/// However, knowing this state is crucial for many update operations. For example, inserting a quad
/// must ensure that the quad is not already present in the store. The purpose of a [LogStateRoot]
/// is to allow accessing such a state that is no longer present in the log.
pub trait HistoricLogStateSource: Send + Sync {
    /// Creates a new [HistoricLogState] for a particular version number.
    fn create_for_version(
        &self,
        version: VersionNumber,
    ) -> Result<Box<dyn HistoricLogState>, HistoricLogStateCreationError>;
}

/// Represents the historic state of the log at a particular version number.
pub trait HistoricLogState {
    /// Filters all existing quads from the given iterator.
    fn filter_existing_quads<'a>(&self, quads: Vec<QuadRef<'a>>) -> Vec<QuadRef<'a>>;

    /// Filters all non-existing quads from the given iterator.
    fn filter_non_existing_quads<'a>(&self, quads: Vec<QuadRef<'a>>) -> Vec<QuadRef<'a>>;
}

/// Used to indicate that there is no log state before the first log entry.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmptyHistoricLogStateSource;

impl HistoricLogStateSource for EmptyHistoricLogStateSource {
    fn create_for_version(
        &self,
        version: VersionNumber,
    ) -> Result<Box<dyn HistoricLogState>, HistoricLogStateCreationError> {
        if version != VersionNumber(0) {
            return Err(HistoricLogStateCreationError::UnavailableVersionNumber).into();
        }

        Ok(Box::new(*self))
    }
}

impl HistoricLogState for EmptyHistoricLogStateSource {
    fn filter_existing_quads<'a>(&self, quads: Vec<QuadRef<'a>>) -> Vec<QuadRef<'a>> {
        quads
    }

    fn filter_non_existing_quads<'a>(&self, quads: Vec<QuadRef<'a>>) -> Vec<QuadRef<'a>> {
        vec![]
    }
}
