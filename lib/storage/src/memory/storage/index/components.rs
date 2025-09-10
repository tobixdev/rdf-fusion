use crate::memory::storage::index::IndexComponentsCreationError;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};

/// Represents a list of *disjunct* index components.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexComponents([IndexComponent; 4]);

impl IndexComponents {
    /// Returns a reference to the inner array.
    pub fn inner(&self) -> &[IndexComponent; 4] {
        &self.0
    }
}

impl Display for IndexComponents {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for component in self.0.iter() {
            write!(f, "{component}")?;
        }
        Ok(())
    }
}

/// Represents what part of an RDF triple is index at the given position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
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

impl Display for IndexComponent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexComponent::GraphName => write!(f, "G"),
            IndexComponent::Subject => write!(f, "S"),
            IndexComponent::Predicate => write!(f, "P"),
            IndexComponent::Object => write!(f, "O"),
        }
    }
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
    use crate::memory::storage::index::IndexComponents;
    use crate::memory::storage::index::components::IndexComponent;

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
