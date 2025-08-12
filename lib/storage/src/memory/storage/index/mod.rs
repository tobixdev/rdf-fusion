use rdf_fusion_encoding::object_id::ObjectIdEncoding;
use std::collections::HashSet;

mod components;
mod error;
mod hash_index;
mod level_data;
mod level_mapping;
mod set;

use crate::memory::encoding::{EncodedActiveGraph, EncodedTermPattern};
use crate::memory::object_id::{EncodedObjectId, DEFAULT_GRAPH_ID};
pub use components::IndexComponents;
pub use error::*;
pub use hash_index::MemHashIndexIterator;
use rdf_fusion_model::Variable;
pub use set::IndexSet;

#[derive(Debug, Clone)]
pub struct IndexedQuad(pub [EncodedObjectId; 4]);

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

#[derive(Debug, Clone)]
pub struct IndexScanInstructions(pub [IndexScanInstruction; 4]);

/// A predicate for filtering object ids.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum ObjectIdScanPredicate {
    /// Checks whether the object id is in the given set.
    In(HashSet<EncodedObjectId>),
    /// Checks whether the object id is *not* in the given set.
    Except(HashSet<EncodedObjectId>),
}

impl ObjectIdScanPredicate {
    /// Indicates whether the predicate restricts to a set of known size.
    ///
    /// For example, [ObjectIdScanPredicate::In] restricts to a known size, while
    /// [ObjectIdScanPredicate::Except] does not, as the universe of object ids is not known.
    pub fn restricts_to_known_size(&self) -> bool {
        match self {
            ObjectIdScanPredicate::In(_) => true,
            ObjectIdScanPredicate::Except(_) => false,
        }
    }

    /// Evaluates the predicate for the given object id.
    pub fn evaluate(&self, object_id: EncodedObjectId) -> bool {
        match self {
            ObjectIdScanPredicate::In(ids) => ids.contains(&object_id),
            ObjectIdScanPredicate::Except(ids) => !ids.contains(&object_id),
        }
    }
}

/// An encoded version of a triple pattern.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum IndexScanInstruction {
    /// Traverses the index level, not binding the elements at this level.
    Traverse(Option<ObjectIdScanPredicate>),
    /// Scans the index level, binding the elements at this level.
    Scan(Option<ObjectIdScanPredicate>),
}

impl IndexScanInstruction {
    pub fn predicate(&self) -> Option<&ObjectIdScanPredicate> {
        match self {
            IndexScanInstruction::Traverse(predicate) => predicate.as_ref(),
            IndexScanInstruction::Scan(predicate) => predicate.as_ref(),
        }
    }
}

impl IndexScanInstruction {
    /// Returns the [IndexScanInstruction] for reading the given [EncodedActiveGraph], also
    /// considering whether the graph name is bound to a `variable`.
    pub fn from_active_graph(
        active_graph: &EncodedActiveGraph,
        variable: Option<&Variable>,
    ) -> IndexScanInstruction {
        let instruction_with_predicate = |predicate: Option<ObjectIdScanPredicate>| {
            if variable.is_some() {
                IndexScanInstruction::Scan(predicate)
            } else {
                IndexScanInstruction::Traverse(predicate)
            }
        };

        match active_graph {
            EncodedActiveGraph::DefaultGraph => {
                let object_ids = HashSet::from([DEFAULT_GRAPH_ID.0]);
                instruction_with_predicate(Some(ObjectIdScanPredicate::In(object_ids)))
            }
            EncodedActiveGraph::AllGraphs => instruction_with_predicate(None),
            EncodedActiveGraph::Union(graphs) => {
                let object_ids = HashSet::from_iter(graphs.iter().map(|g| g.0));
                instruction_with_predicate(Some(ObjectIdScanPredicate::In(object_ids)))
            }
            EncodedActiveGraph::AnyNamedGraph => {
                let object_ids = HashSet::from([DEFAULT_GRAPH_ID.0]);
                instruction_with_predicate(Some(ObjectIdScanPredicate::Except(
                    object_ids,
                )))
            }
        }
    }
}

impl From<EncodedTermPattern> for IndexScanInstruction {
    fn from(value: EncodedTermPattern) -> Self {
        match value {
            EncodedTermPattern::ObjectId(object_id) => IndexScanInstruction::Traverse(
                Some(ObjectIdScanPredicate::In(HashSet::from([object_id]))),
            ),
            EncodedTermPattern::Variable(_) => IndexScanInstruction::Scan(None),
        }
    }
}
