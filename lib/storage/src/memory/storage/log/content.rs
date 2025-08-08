use crate::memory::encoding::{EncodedQuad, EncodedQuadArray};
use crate::memory::object_id::{EncodedObjectId, GraphEncodedObjectId};
use crate::memory::storage::log::VersionNumber;
use datafusion::arrow::array::{Array, StructArray, UnionArray};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// A single entry in the log.
///
/// Each log entry represents one action that can happen to the store and represents a single
/// transaction. It is currently not possible to, for example, clear a graph and insert a quad in
/// the same transaction.
pub struct MemLogEntry {
    /// The version number of this transaction.
    pub version_number: VersionNumber,
    /// The action that should be performed.
    pub action: MemLogEntryAction,
}

/// The target of a clear operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClearTarget {
    /// Creates a named graph or the default graph.
    Graph(GraphEncodedObjectId),
    /// Clears all named graphs.
    AllNamedGraphs,
    /// Clears all graphs.
    AllGraphs,
}

/// Represents the action that should be performed by a transaction.
pub enum MemLogEntryAction {
    /// Updates the store. The quads are inserted and deleted.
    Update(MemLogUpdateArray),
    /// Clears the store.
    Clear(ClearTarget),
    /// Creates a named graph.
    CreateNamedGraph(EncodedObjectId),
    /// Completely drops a named graph.
    DropGraph(EncodedObjectId),
}

/// Holds the number of triples for each graph.
pub struct GraphLen {
    /// Mapping from graph to the change.
    pub graph: HashMap<GraphEncodedObjectId, usize>,
}

impl GraphLen {
    /// Create a new [GraphLen].
    fn new() -> Self {
        Self {
            graph: HashMap::new(),
        }
    }
}

/// Holds the actual log entries.
pub struct MemLogContent {
    logs: Vec<MemLogEntry>,
}

impl MemLogContent {
    /// Creates a new [MemLogContent].
    pub fn new() -> Self {
        Self { logs: vec![] }
    }

    /// Returns the list of contained [MemLogEntry]s.
    pub fn log_entries(&self) -> &[MemLogEntry] {
        &self.logs
    }

    /// Appends a [MemLogEntry].
    pub fn append_log_entry(&mut self, log_array: MemLogEntry) {
        self.logs.push(log_array);
    }

    /// Computes the contained quads based on the log entries.
    pub fn compute_quads(&self, version_number: VersionNumber) -> HashSet<EncodedQuad> {
        let mut quads = HashSet::new();

        for entry in self.relevant_log_entries(version_number) {
            match &entry.action {
                MemLogEntryAction::Update(log_array) => {
                    for quad in &log_array.insertions() {
                        quads.insert(quad.clone());
                    }

                    for quad in &log_array.deletions() {
                        quads.remove(&quad);
                    }
                }
                MemLogEntryAction::Clear(ClearTarget::Graph(graph)) => {
                    // Remove all quads from the given graph.
                    quads.retain(|quad| quad.graph_name.0 != (*graph).into());
                }
                MemLogEntryAction::Clear(ClearTarget::AllNamedGraphs) => {
                    // Remove all quads from the given graph.
                    quads.retain(|quad| quad.graph_name.0.is_none());
                }
                MemLogEntryAction::Clear(ClearTarget::AllGraphs) => {
                    quads.clear();
                }
                MemLogEntryAction::DropGraph(graph) => {
                    // Remove all quads from the given graph.
                    quads.retain(|quad| quad.graph_name.0 != Some(*graph));
                }
                MemLogEntryAction::CreateNamedGraph(_) => {
                    // Do nothing.
                }
            }
        }

        quads
    }

    /// Counts the changes in the log up until the version number of the snapshot.
    pub fn len(&self, version_number: VersionNumber) -> GraphLen {
        let mut mapping = GraphLen::new();

        for entry in self
            .log_entries()
            .iter()
            .take_while(|e| e.version_number <= version_number)
        {
            match &entry.action {
                MemLogEntryAction::Update(update) => {
                    for insert in &update.insertions() {
                        let count = mapping.graph.entry(insert.graph_name).or_insert(0);
                        *count += 1;
                    }

                    for deletion in &update.deletions() {
                        let count = mapping.graph.entry(deletion.graph_name).or_insert(0);
                        *count -= 1;
                    }
                }
                MemLogEntryAction::Clear(target) => match target {
                    ClearTarget::Graph(g) => {
                        mapping.graph.remove(g);
                    }
                    ClearTarget::AllNamedGraphs => {
                        mapping.graph.retain(|k, _| k.0.is_none());
                    }
                    ClearTarget::AllGraphs => {
                        mapping.graph.clear();
                    }
                },
                MemLogEntryAction::CreateNamedGraph(g) => {
                    mapping.graph.entry(Some(*g).into()).or_insert(0);
                }
                MemLogEntryAction::DropGraph(g) => {
                    mapping.graph.remove(&(Some(*g).into()));
                }
            }
        }

        mapping
    }

    /// Checks if the given graph in the log.
    pub fn contains_named_graph(
        &self,
        graph: EncodedObjectId,
        version_number: VersionNumber,
    ) -> bool {
        self.len(version_number)
            .graph
            .contains_key(&(Some(graph).into()))
    }

    fn relevant_log_entries(
        &self,
        version_number: VersionNumber,
    ) -> impl Iterator<Item = &MemLogEntry> {
        self.logs
            .iter()
            .take_while(move |entry| entry.version_number <= version_number)
    }
}

impl Default for MemLogContent {
    fn default() -> Self {
        MemLogContent::new()
    }
}

/// A [MemLogUpdateArray] contains the logs of a single transaction.
///
/// There are multiple types of log entries.
///
/// # Insert & Delete Entries
///
/// Each insert (delete) entry marks the insertion (deletion) of a single quad.
///
/// Each entry has four fields that correspond to the parts of the quad: graph, subject, predicate,
/// object.
pub struct MemLogUpdateArray {
    /// The union array that holds the actual log entries.
    pub array: Arc<UnionArray>,
}

impl MemLogUpdateArray {
    /// Get a reference to the list of insertions.
    ///
    /// The list of insertions is disjunct from the list of deletions.
    pub fn insertions(&self) -> EncodedQuadArray<'_> {
        let array = self
            .array
            .child(0)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Arrays are fixed");
        EncodedQuadArray::new(array)
    }

    /// Get a reference to the list of deletions.
    ///
    /// The list of deletions is disjunct from the list of insertions.
    pub fn deletions(&self) -> EncodedQuadArray<'_> {
        let array = self
            .array
            .child(1)
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("Arrays are fixed");
        EncodedQuadArray::new(array)
    }
}
