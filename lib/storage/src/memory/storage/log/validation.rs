use crate::memory::encoding::EncodedQuad;
use crate::memory::object_id::EncodedObjectId;
use crate::memory::storage::log::content::{
    ClearTarget, MemLogContent, MemLogEntryAction, MemLogUpdateArray,
};
use crate::memory::storage::VersionNumber;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// Validates the [MemLogContent].
///
/// The following rules are checked:
/// - Quads are not inserted twice, except they have been deleted.
/// - Quads can only be deleted after they have been inserted.
/// - A single update action does not contain the same quad in the insertions and deletions lists.
/// - A named graph can only be created if it does not exist yet.
/// - A named graph can only be deleted after it has been created
pub fn validate_mem_log(content: &MemLogContent) -> Result<(), Vec<LogCorruptionError>> {
    let mut validation_state = ValidationState::new();

    for entry in content.log_entries() {
        match &entry.action {
            MemLogEntryAction::Update(update) => {
                validation_state.validate_update(entry.version_number, update);
            }
            MemLogEntryAction::Clear(target) => {
                validation_state.validate_clear(entry.version_number, *target);
            }
            MemLogEntryAction::CreateNamedGraph(new_graph) => {
                validation_state
                    .validate_create_named_graph(entry.version_number, *new_graph);
            }
            MemLogEntryAction::DropGraph(graph) => {
                validation_state.validate_drop_named_graph(entry.version_number, *graph);
            }
        }
    }

    if validation_state.corruptions.is_empty() {
        Ok(())
    } else {
        Err(validation_state.corruptions)
    }
}

struct ValidationState {
    /// The current version number.
    current_version: VersionNumber,
    /// The quads that are currently present in the store.
    existing_quads: HashSet<EncodedQuad>,
    /// The named graphs that are currently present in the store.
    existing_named_graphs: HashSet<EncodedObjectId>,
    /// The corruptions that have been found so far.
    corruptions: Vec<LogCorruptionError>,
}

impl ValidationState {
    /// Creates a new [ValidationState].
    fn new() -> ValidationState {
        ValidationState {
            current_version: VersionNumber(0),
            corruptions: Vec::new(),
            existing_quads: HashSet::new(),
            existing_named_graphs: HashSet::new(),
        }
    }

    /// Validate update
    fn validate_update(
        &mut self,
        version_number: VersionNumber,
        update: &MemLogUpdateArray,
    ) {
        self.process_version_number(version_number);

        let insertions = update.insertions().into_iter().collect::<HashSet<_>>();
        let deletions = update.deletions().into_iter().collect::<HashSet<_>>();

        if insertions.is_empty() && deletions.is_empty() {
            self.push_corruption(LogCorruption::EmptyUpdate)
        }

        if insertions.intersection(&deletions).count() > 0 {
            self.push_corruption(LogCorruption::QuadsInsertedAndDeleted);
        }

        if insertions.len() != update.insertions().len() {
            self.push_corruption(LogCorruption::UpdateContainsDuplicateInsertions);
        }

        if deletions.len() != update.deletions().len() {
            self.push_corruption(LogCorruption::UpdateContainsDuplicateDeletions);
        }

        if self.existing_quads.intersection(&insertions).count() > 0 {
            self.push_corruption(LogCorruption::ExistingQuadInserted)
        }

        if deletions.difference(&self.existing_quads).count() > 0 {
            self.push_corruption(LogCorruption::QuadDeletedBeforeInserted)
        }

        self.existing_named_graphs.extend(
            insertions
                .iter()
                .filter_map(|q| q.graph_name.try_as_encoded_object_id()),
        );
        self.existing_quads.retain(|q| !deletions.contains(q));
        self.existing_quads.extend(insertions);
    }

    fn validate_clear(&mut self, version_number: VersionNumber, target: ClearTarget) {
        self.process_version_number(version_number);

        match target {
            ClearTarget::Graph(to_clear) => {
                if let Some(to_clear) = to_clear.try_as_encoded_object_id() {
                    if !self.existing_named_graphs.contains(&to_clear) {
                        self.push_corruption(LogCorruption::GraphClearedBeforeCreated)
                    }
                }

                self.existing_quads.retain(|g| g.graph_name != to_clear);
            }
            ClearTarget::AllNamedGraphs => {
                self.existing_quads
                    .retain(|g| g.graph_name.is_default_graph());
            }
            ClearTarget::AllGraphs => {
                self.existing_quads.clear();
            }
        }
    }

    fn validate_create_named_graph(
        &mut self,
        version_number: VersionNumber,
        graph: EncodedObjectId,
    ) {
        self.process_version_number(version_number);

        if self.existing_named_graphs.contains(&graph) {
            self.push_corruption(LogCorruption::ExistingNamedGraphCreated)
        }

        self.existing_named_graphs.insert(graph);
    }

    fn validate_drop_named_graph(
        &mut self,
        version_number: VersionNumber,
        to_clear: EncodedObjectId,
    ) {
        self.process_version_number(version_number);

        if !self.existing_named_graphs.contains(&to_clear) {
            self.push_corruption(LogCorruption::GraphDroppedBeforeCreated)
        }

        self.existing_quads.retain(|q| q.graph_name.0 != to_clear);
        self.existing_named_graphs.remove(&to_clear);
    }

    fn process_version_number(&mut self, version_number: VersionNumber) {
        if version_number <= self.current_version {
            self.push_corruption(LogCorruption::NonMonotonicVersionNumber);
        }

        if version_number.0 > self.current_version.0 + 1 {
            self.push_corruption(LogCorruption::MissingVersionNumber);
        }

        self.current_version = version_number;
    }

    fn push_corruption(&mut self, corruption: LogCorruption) {
        self.corruptions.push(LogCorruptionError {
            version: self.current_version,
            corruption,
        })
    }
}

#[derive(Debug, Error)]
#[error("Log corruption in version {version}: {corruption}")]
pub struct LogCorruptionError {
    /// The offending version number.
    version: VersionNumber,
    /// The corruption.
    corruption: LogCorruption,
}

/// An enum for distinguishing between log corruptions.
#[derive(Debug, Clone)]
pub enum LogCorruption {
    /// An update entry is empty.
    EmptyUpdate,
    /// An existing quad was inserted.
    ExistingQuadInserted,
    /// A quad was deleted before it was inserted.
    QuadDeletedBeforeInserted,
    /// Quads were inserted twice in a single update.
    UpdateContainsDuplicateInsertions,
    /// Quads were deleted twice in a single update.
    UpdateContainsDuplicateDeletions,
    /// Quads were inserted and deleted in the same update.
    QuadsInsertedAndDeleted,
    /// An existing named graph was created.
    ExistingNamedGraphCreated,
    /// A named graph was deleted that did not exist.
    GraphClearedBeforeCreated,
    /// A named graph was deleted that did not exist.
    GraphDroppedBeforeCreated,
    /// The version number is not monotonically increasing.
    NonMonotonicVersionNumber,
    /// A version number is missing.
    MissingVersionNumber,
}

impl Display for LogCorruption {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            LogCorruption::EmptyUpdate => {
                write!(f, "The update was empty")
            }
            LogCorruption::ExistingQuadInserted => {
                write!(f, "Quad was inserted twice")
            }
            LogCorruption::QuadDeletedBeforeInserted => {
                write!(f, "Quad was deleted before it was inserted")
            }
            LogCorruption::UpdateContainsDuplicateInsertions => {
                write!(f, "Update contains duplicate insertions")
            }
            LogCorruption::UpdateContainsDuplicateDeletions => {
                write!(f, "Update contains duplicate deletions")
            }
            LogCorruption::QuadsInsertedAndDeleted => {
                write!(f, "Quads were inserted and deleted in the same update")
            }
            LogCorruption::ExistingNamedGraphCreated => {
                write!(f, "Named graph was created twice")
            }
            LogCorruption::GraphClearedBeforeCreated => {
                write!(f, "Named graph was cleared before it was created")
            }
            LogCorruption::GraphDroppedBeforeCreated => {
                write!(f, "Named graph was dropped before it was created")
            }
            LogCorruption::NonMonotonicVersionNumber => {
                write!(f, "The version number is not monotonically increasing")
            }
            LogCorruption::MissingVersionNumber => {
                write!(f, "A version number is missing")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memory::object_id::DEFAULT_GRAPH_ID;
    use crate::memory::storage::log::builder::MemLogEntryBuilder;
    use crate::memory::storage::log::content::MemLogEntry;
    use insta::assert_snapshot;

    #[test]
    fn invalid_duplicate_insertion_without_deletion() {
        let mut log = MemLogContent::new();
        let quad = make_quad(1, 1, 1, 1);

        log.append_log_entry(make_update(VersionNumber(1), vec![quad.clone()], vec![]));
        log.append_log_entry(make_update(VersionNumber(2), vec![quad.clone()], vec![]));

        let result = validate_mem_log(&log);
        assert_snapshot!(format_corruption_error(result), @r"
        Corruption(s) detected in log:
        - Log corruption in version 2: Quad was inserted twice
        ");
    }

    #[test]
    fn invalid_deletion_before_insertion() {
        let mut log = MemLogContent::new();
        let quad = make_quad(1, 1, 1, 1);

        log.append_log_entry(make_update(VersionNumber(1), vec![], vec![quad.clone()]));

        let result = validate_mem_log(&log);
        assert_snapshot!(format_corruption_error(result), @r"
        Corruption(s) detected in log:
        - Log corruption in version 1: Quad was deleted before it was inserted
        ");
    }

    #[test]
    fn invalid_insertion_and_deletion_in_same_update() {
        let mut log = MemLogContent::new();
        let quad = make_quad(1, 1, 1, 1);

        log.append_log_entry(make_update(
            VersionNumber(1),
            vec![quad.clone()],
            vec![quad.clone()],
        ));

        let result = validate_mem_log(&log);
        assert_snapshot!(format_corruption_error(result), @r"
        Corruption(s) detected in log:
        - Log corruption in version 1: Quads were inserted and deleted in the same update
        - Log corruption in version 1: Quad was deleted before it was inserted
        ");
    }

    #[test]
    fn invalid_duplicate_named_graph_creation() {
        let mut log = MemLogContent::new();
        let graph = EncodedObjectId::from(1);

        log.append_log_entry(MemLogEntry {
            version_number: VersionNumber(1),
            action: MemLogEntryAction::CreateNamedGraph(graph),
        });

        log.append_log_entry(MemLogEntry {
            version_number: VersionNumber(2),
            action: MemLogEntryAction::CreateNamedGraph(graph),
        });

        let result = validate_mem_log(&log);
        assert_snapshot!(format_corruption_error(result), @r"
        Corruption(s) detected in log:
        - Log corruption in version 2: Named graph was created twice
        ");
    }

    #[test]
    fn invalid_drop_graph_before_creation() {
        let mut log = MemLogContent::new();
        let graph = EncodedObjectId::from(1);

        log.append_log_entry(MemLogEntry {
            version_number: VersionNumber(1),
            action: MemLogEntryAction::DropGraph(graph),
        });

        let result = validate_mem_log(&log);
        assert_snapshot!(format_corruption_error(result), @r"
        Corruption(s) detected in log:
        - Log corruption in version 1: Named graph was dropped before it was created
        ");
    }

    #[test]
    fn invalid_clear_graph_before_creation() {
        let mut log = MemLogContent::new();
        let graph = EncodedObjectId::from(1);

        log.append_log_entry(MemLogEntry {
            version_number: VersionNumber(1),
            action: MemLogEntryAction::Clear(ClearTarget::Graph(graph.into())),
        });

        let result = validate_mem_log(&log);
        assert_snapshot!(format_corruption_error(result), @r"
        Corruption(s) detected in log:
        - Log corruption in version 1: Named graph was cleared before it was created
        ");
    }

    #[test]
    fn invalid_missing_version_number() {
        let mut log = MemLogContent::new();
        let quad = make_quad(1, 1, 1, 1);

        log.append_log_entry(make_update(VersionNumber(1), vec![quad.clone()], vec![]));
        log.append_log_entry(make_update(VersionNumber(3), vec![], vec![quad.clone()]));

        let result = validate_mem_log(&log);
        assert_snapshot!(format_corruption_error(result), @r"
        Corruption(s) detected in log:
        - Log corruption in version 1: A version number is missing
        ");
    }

    #[test]
    fn invalid_duplicate_version_number() {
        let mut log = MemLogContent::new();
        let quad = make_quad(1, 1, 1, 1);

        log.append_log_entry(make_update(VersionNumber(1), vec![quad.clone()], vec![]));
        log.append_log_entry(make_update(VersionNumber(0), vec![], vec![quad.clone()]));

        let result = validate_mem_log(&log);
        assert_snapshot!(format_corruption_error(result), @r"
        Corruption(s) detected in log:
        - Log corruption in version 1: The version number is not monotonically increasing
        ");
    }

    #[test]
    fn valid_insertion_then_deletion() {
        let mut log = MemLogContent::new();
        let quad = make_quad(1, 1, 1, 1);

        log.append_log_entry(make_update(VersionNumber(1), vec![quad.clone()], vec![]));
        log.append_log_entry(make_update(VersionNumber(2), vec![], vec![quad.clone()]));
        log.append_log_entry(make_update(VersionNumber(3), vec![quad.clone()], vec![]));

        let result = validate_mem_log(&log);
        assert!(result.is_ok());
    }

    #[test]
    fn valid_insertion_then_clear_then_insertion() {
        let mut log = MemLogContent::new();
        let quad = make_quad(1, 1, 1, 1);

        log.append_log_entry(make_update(VersionNumber(1), vec![quad.clone()], vec![]));
        log.append_log_entry(MemLogEntry {
            version_number: VersionNumber(2),
            action: MemLogEntryAction::Clear(ClearTarget::AllGraphs),
        });
        log.append_log_entry(make_update(VersionNumber(3), vec![quad.clone()], vec![]));

        let result = validate_mem_log(&log);
        assert!(result.is_ok());
    }

    #[test]
    fn valid_insertion_then_clear_then_insertion_default_graph() {
        let mut log = MemLogContent::new();
        let quad = EncodedQuad {
            graph_name: DEFAULT_GRAPH_ID,
            subject: 1.into(),
            predicate: 2.into(),
            object: 3.into(),
        };

        log.append_log_entry(make_update(VersionNumber(1), vec![quad.clone()], vec![]));
        log.append_log_entry(MemLogEntry {
            version_number: VersionNumber(2),
            action: MemLogEntryAction::Clear(ClearTarget::Graph(DEFAULT_GRAPH_ID)),
        });
        log.append_log_entry(make_update(VersionNumber(3), vec![quad.clone()], vec![]));

        let result = validate_mem_log(&log);
        assert!(result.is_ok());
    }

    #[test]
    fn valid_insertion_then_drop_then_insertion() {
        let mut log = MemLogContent::new();
        let quad = make_quad(1, 1, 1, 1);

        log.append_log_entry(make_update(VersionNumber(1), vec![quad.clone()], vec![]));
        log.append_log_entry(MemLogEntry {
            version_number: VersionNumber(2),
            action: MemLogEntryAction::DropGraph(1.into()),
        });
        log.append_log_entry(make_update(VersionNumber(3), vec![quad.clone()], vec![]));

        let result = validate_mem_log(&log);
        assert!(result.is_ok());
    }

    fn make_quad(g: u32, s: u32, p: u32, o: u32) -> EncodedQuad {
        EncodedQuad {
            graph_name: EncodedObjectId::from(g).into(),
            subject: s.into(),
            predicate: p.into(),
            object: o.into(),
        }
    }

    fn make_update(
        version_number: VersionNumber,
        insertions: Vec<EncodedQuad>,
        deletions: Vec<EncodedQuad>,
    ) -> MemLogEntry {
        let mut builder = MemLogEntryBuilder::new();

        for insertion in insertions {
            builder.append_insertion(&insertion).unwrap();
        }

        for deletion in deletions {
            builder.append_deletion(&deletion).unwrap();
        }

        builder.build(version_number).unwrap().unwrap()
    }

    fn format_corruption_error(result: Result<(), Vec<LogCorruptionError>>) -> String {
        match result {
            Ok(_) => panic!("Expected corruption, but got Ok"),
            Err(errors) => {
                let mut error = String::from("Corruption(s) detected in log:\n");
                for err in errors {
                    error.push_str(&format!("- {}\n", err));
                }
                error
            }
        }
    }
}
