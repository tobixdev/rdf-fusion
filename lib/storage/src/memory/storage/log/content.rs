use crate::memory::encoding::{EncodedQuad, EncodedQuadArray};
use crate::memory::object_id::{EncodedObjectId, GraphEncodedObjectId};
use crate::memory::storage::log::builder::MemLogEntryBuilderError;
use crate::memory::storage::log::VersionNumber;
use datafusion::arrow::array::{Array, StructArray, UnionArray};
use rdf_fusion_common::error::StorageError;
use std::collections::HashSet;
use std::sync::Arc;

/// Holds the actual log entries.
pub struct MemLogContent {
    logs: Vec<MemLogEntry>,
}

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
    CreateEmptyNamedGraph(EncodedObjectId),
    /// Completely drops a named graph.
    DropGraph(EncodedObjectId),
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
                MemLogEntryAction::CreateEmptyNamedGraph(_) => {
                    // Do nothing.
                }
            }
        }

        quads
    }

    pub fn contains_named_graph(
        &self,
        graph: EncodedObjectId,
        version_number: VersionNumber,
    ) -> bool {
        let relevant_log_entries = self
            .relevant_log_entries(version_number)
            .collect::<Vec<_>>();

        // Iterate backwards through the log entries to find the last relevant update
        for entry in relevant_log_entries.iter().rev() {
            match &entry.action {
                // If we see an update or a deletion of a quad in the given graph, we know that
                // the graph exists. Also, if the graph becomes empty after the update, it exists.
                MemLogEntryAction::Update(update) => {
                    let inserted = update
                        .insertions()
                        .into_iter()
                        .any(|quad| quad.graph_name.0 == graph.into());
                    let deleted = update
                        .deletions()
                        .into_iter()
                        .any(|quad| quad.graph_name.0 == Some(graph));
                    if inserted || deleted {
                        return true;
                    }
                }
                // Clear does not drop the graph, therefore it must exist.
                MemLogEntryAction::Clear(ClearTarget::Graph(cleared))
                    if (*cleared).0 == Some(graph) =>
                {
                    return true;
                }
                // Creating an empty graph creates the graph.
                MemLogEntryAction::CreateEmptyNamedGraph(created)
                    if *created == graph =>
                {
                    return true;
                }
                // Dropping the graph deletes it.
                MemLogEntryAction::DropGraph(dropped) if *dropped == graph => {
                    return false;
                }
                _ => {}
            }
        }

        // If we reach this point, the graph was never created or inserted into.
        false
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
