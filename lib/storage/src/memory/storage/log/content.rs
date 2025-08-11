use crate::memory::encoding::{EncodedQuad, EncodedQuadArray};
use crate::memory::object_id::{EncodedGraphObjectId, EncodedObjectId};
use crate::memory::storage::VersionNumber;
use datafusion::arrow::array::{Array, StructArray, UnionArray};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

/// A single entry in the log.
///
/// Each log entry represents one action that can happen to the store and represents a single
/// transaction. It is currently not possible to, for example, clear a graph and insert a quad in
/// the same transaction.
#[derive(Debug)]
pub struct MemLogEntry {
    /// The version number of this transaction.
    pub version_number: VersionNumber,
    /// The action that should be performed.
    pub action: MemLogEntryAction,
}

/// The target of a clear operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ClearTarget {
    /// Creates a named graph or the default graph.
    Graph(EncodedGraphObjectId),
    /// Clears all named graphs.
    AllNamedGraphs,
    /// Clears all graphs.
    AllGraphs,
}

/// Represents the action that should be performed by a transaction.
#[derive(Debug)]
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

/// Holds the actual log entries.
pub struct MemLogContent {
    logs: Vec<MemLogEntry>,
}

/// Reflects the changes that have been made to the store.
///
/// The changes are constructed in such a way that they are directly usable by a query engine. For
/// example, a quad that is inserted and then deleted again will not be contained in
/// [Self::inserted].
#[derive(Debug)]
pub struct LogChanges {
    /// The version number of the first transaction that has been applied.
    pub from_version: VersionNumber,
    /// The version number of the last transaction that has been applied.
    pub to_version: VersionNumber,
    /// The quads that have been inserted.
    pub inserted: HashSet<EncodedQuad>,
    /// The quads that have been deleted.
    ///
    /// Note that if there are elements in [Self::cleared] that already imply the deletion of a
    /// quad, this quad will not be part of this set. This is done because query evaluation must
    /// consider the cleared and dropped graphs anyway.
    pub deleted: HashSet<EncodedQuad>,
    /// Contains the set of clear statements. Note that [Self::inserted] may contain quads from
    /// these graphs as they could have been inserted later.
    pub cleared: HashSet<ClearTarget>,
    /// Contains the set of dropped named graphs. This is different from [Self::cleared] when
    /// returning the list of named graphs.
    pub dropped_named_graphs: HashSet<EncodedObjectId>,
    /// Contains the set of created named graphs.
    pub created_named_graphs: HashSet<EncodedObjectId>,
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

    /// Appends a [MemLogEntry].
    pub fn clear_log_until(&mut self, version_number: VersionNumber) {
        let truncate_index = self
            .logs
            .iter()
            .position(|entry| entry.version_number > version_number)
            .unwrap_or(self.logs.len());
        self.logs.truncate(truncate_index);
    }

    /// Computes the contained quads based on the log entries.
    pub fn compute_changes(&self, version_number: VersionNumber) -> Option<LogChanges> {
        if self.logs.is_empty() {
            return None;
        }

        let mut inserted = HashSet::new();
        let mut deleted = HashSet::new();
        let mut cleared = HashSet::new();
        let mut dropped_named_graphs = HashSet::new();
        let mut created_named_graphs = HashSet::new();

        for entry in self.relevant_log_entries(version_number) {
            match &entry.action {
                MemLogEntryAction::Update(log_array) => {
                    let new_inserted =
                        log_array.insertions().into_iter().collect::<HashSet<_>>();
                    let new_deleted =
                        log_array.deletions().into_iter().collect::<HashSet<_>>();

                    // Remove all quads that have been inserted and deleted again.
                    inserted.retain(|quad| !new_deleted.contains(quad));
                    deleted.retain(|quad| !new_inserted.contains(quad));

                    // Add all quads that have been inserted and deleted.
                    inserted.extend(new_inserted);
                    deleted.extend(new_deleted);
                }
                MemLogEntryAction::Clear(ClearTarget::Graph(graph)) => {
                    inserted.retain(|quad| quad.graph_name != *graph);
                    deleted.retain(|quad| quad.graph_name != *graph);
                    cleared.insert(ClearTarget::Graph(*graph));
                }
                MemLogEntryAction::Clear(ClearTarget::AllNamedGraphs) => {
                    inserted.retain(|quad| quad.graph_name.is_default_graph());
                    deleted.retain(|quad| quad.graph_name.is_default_graph());
                    cleared.insert(ClearTarget::AllNamedGraphs);
                }
                MemLogEntryAction::Clear(ClearTarget::AllGraphs) => {
                    inserted.clear();
                    deleted.clear();
                    cleared.insert(ClearTarget::AllGraphs);
                }
                MemLogEntryAction::DropGraph(graph) => {
                    inserted.retain(|quad| quad.graph_name.0 != *graph);
                    deleted.retain(|quad| quad.graph_name.0 != *graph);

                    dropped_named_graphs.insert(*graph);
                    created_named_graphs.remove(graph);
                }
                MemLogEntryAction::CreateNamedGraph(graph) => {
                    created_named_graphs.insert(*graph);
                    dropped_named_graphs.remove(graph);
                }
            }
        }

        let first_version_number = self.logs[0].version_number;
        let last_version_number = self.logs.last().unwrap().version_number;
        Some(LogChanges {
            from_version: first_version_number,
            to_version: last_version_number,
            inserted,
            deleted,
            cleared,
            created_named_graphs,
            dropped_named_graphs,
        })
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

impl Debug for MemLogContent {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemLogContent").finish()
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
#[derive(Debug)]
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
