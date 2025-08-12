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
pub use components::IndexComponent;
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

/// An encoded version of a triple pattern.
#[derive(Eq, PartialEq, Debug, Clone)]
pub enum IndexScanInstruction {
    /// Scans
    ScanOnly {
        bind: bool,
        filter: HashSet<EncodedObjectId>,
    },
    /// Scans all elements in the current level except the given ones.
    ///
    /// [ScanExcept] with an empty filter is used for scanning an entire index level.
    ScanExcept {
        bind: bool,
        filter: HashSet<EncodedObjectId>,
    },
}

impl IndexScanInstruction {
    pub fn has_predefined_filter(&self) -> bool {
        match self {
            IndexScanInstruction::ScanOnly { .. } => true,
            IndexScanInstruction::ScanExcept { .. } => false,
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
        let bind = variable.is_some();
        match active_graph {
            EncodedActiveGraph::DefaultGraph => IndexScanInstruction::ScanOnly {
                bind,
                filter: HashSet::from([DEFAULT_GRAPH_ID.0]),
            },
            EncodedActiveGraph::AllGraphs => IndexScanInstruction::ScanExcept {
                bind,
                filter: HashSet::new(),
            },
            EncodedActiveGraph::Union(graphs) => IndexScanInstruction::ScanOnly {
                bind,
                filter: HashSet::from_iter(graphs.iter().map(|g| g.0)),
            },
            EncodedActiveGraph::AnyNamedGraph => IndexScanInstruction::ScanExcept {
                bind,
                filter: HashSet::from([DEFAULT_GRAPH_ID.0]),
            },
        }
    }
}

impl From<EncodedTermPattern> for IndexScanInstruction {
    fn from(value: EncodedTermPattern) -> Self {
        match value {
            EncodedTermPattern::ObjectId(object_id) => IndexScanInstruction::ScanOnly {
                bind: false,
                filter: HashSet::from([object_id]),
            },
            EncodedTermPattern::Variable(_) => IndexScanInstruction::ScanExcept {
                bind: true,
                filter: HashSet::new(),
            },
        }
    }
}
