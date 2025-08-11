use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use std::collections::HashSet;
use thiserror::Error;

mod error;
mod data;
mod level;
mod set;
mod state_root;

pub use set::IndexSet;

/// Holds the configuration for the index.
#[derive(Debug, Clone)]
pub struct IndexConfiguration {
    /// The object id encoding.
    pub object_id_encoding: ObjectIdEncoding,
    /// The desired batch size. This iterator only provides a best-effort service for adhering to
    /// the batch size.
    pub batch_size: usize,
    /// Differentiates between multiple configurations (e.g., SPO, PSO).
    pub components: IndexComponents,
}

/// Represents what part of an RDF triple is index at the given position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IndexComponent {
    /// The graph name
    GraphName,
    /// The subject
    Subject,
    /// The predicate
    Predicate,
    /// The object
    Object,
}

impl IndexComponent {
    /// Returns the index of the component in an GSPO quad pattern.
    pub fn gspo_index(&self) -> usize {
        match self {
            IndexComponent::GraphName => 0,
            IndexComponent::Subject => 1,
            IndexComponent::Predicate => 2,
            IndexComponent::Object => 3,
        }
    }
}

/// Represents a list of *disjunct* index components.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexComponents([IndexComponent; 4]);

#[derive(Debug, Error)]
#[error("Duplicate indexed component given.")]
pub struct IndexComponentsCreationError;

impl IndexComponents {
    /// A GSPO index.
    pub const GSPO: IndexComponents = IndexComponents([
        IndexComponent::GraphName,
        IndexComponent::Subject,
        IndexComponent::Predicate,
        IndexComponent::Object,
    ]);

    /// A GPOS index.
    pub const GPOS: IndexComponents = IndexComponents([
        IndexComponent::GraphName,
        IndexComponent::Predicate,
        IndexComponent::Object,
        IndexComponent::Subject,
    ]);

    /// A GPSO index.
    pub const GOSP: IndexComponents = IndexComponents([
        IndexComponent::GraphName,
        IndexComponent::Object,
        IndexComponent::Subject,
        IndexComponent::Predicate,
    ]);

    /// Tries to create a new [IndexConfiguration].
    ///
    /// Returns an error if an [IndexComponent] appears more than once.
    pub fn try_new(
        components: [IndexComponent; 4],
    ) -> Result<Self, IndexComponentsCreationError> {
        let distinct = components.iter().collect::<HashSet<_>>();
        if distinct.len() != components.len() {
            return Err(IndexComponentsCreationError);
        }

        Ok(IndexComponents(components))
    }
}

#[cfg(test)]
mod tests {
    use crate::memory::storage::index::{IndexComponent, IndexComponents};

    #[test]
    fn index_configuration_accepts_unique_components() {
        let ok = IndexComponents::try_new([
            IndexComponent::GraphName,
            IndexComponent::Subject,
            IndexComponent::Predicate,
            IndexComponent::Object,
        ]);
        assert!(ok.is_ok());
    }

    #[test]
    fn index_configuration_rejects_duplicate_components() {
        let err = IndexComponents::try_new([
            IndexComponent::GraphName,
            IndexComponent::Subject,
            IndexComponent::Subject,
            IndexComponent::Object,
        ]);
        assert!(err.is_err());
    }
}
