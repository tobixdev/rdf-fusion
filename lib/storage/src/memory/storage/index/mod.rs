use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use std::collections::HashSet;
use thiserror::Error;

mod error;
mod index;
mod level;

/// Holds the configuration for the index.
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
    /// The subject
    Subject,
    /// The predicate
    Predicate,
    /// The object
    Object,
}

/// Represents a list of *disjunct* index components.
pub struct IndexComponents([IndexComponent; 3]);

#[derive(Debug, Error)]
#[error("Duplicate indexed component given.")]
pub struct IndexComponentsCreationError;

impl IndexComponents {
    /// Tries to create a new [IndexConfiguration].
    ///
    /// Returns an error if an [IndexComponent] appears more than once.
    pub fn try_new(
        components: [IndexComponent; 3],
    ) -> Result<Self, IndexComponentsCreationError> {
        let distinct = components.iter().collect::<HashSet<_>>();
        if distinct.len() != 3 {
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
            IndexComponent::Subject,
            IndexComponent::Predicate,
            IndexComponent::Object,
        ]);
        assert!(ok.is_ok());
    }

    #[test]
    fn index_configuration_rejects_duplicate_components() {
        let err = IndexComponents::try_new([
            IndexComponent::Subject,
            IndexComponent::Subject,
            IndexComponent::Object,
        ]);
        assert!(err.is_err());
    }
}
