use crate::memory::encoding::{EncodedQuad, EncodedQuadArray};
use crate::memory::storage::log::VersionNumber;
use datafusion::arrow::array::{
    Array, StructArray, StructBuilder, UInt32Builder, UnionArray,
};
use datafusion::arrow::datatypes::{Field, UnionFields};
use rdf_fusion_common::error::StorageError;
use std::collections::HashSet;
use std::num::TryFromIntError;
use std::sync::Arc;
use thiserror::Error;

/// Holds the actual log entries.
pub struct MemLogContent {
    logs: Vec<MemLogArray>,
}

impl MemLogContent {
    /// Creates a new [MemLogContent].
    pub fn new() -> Self {
        Self { logs: vec![] }
    }

    /// Returns the list of contained [MemLogArray]s.
    pub fn log_arrays(&self) -> &[MemLogArray] {
        &self.logs
    }

    /// Appends a [MemLogArray].
    pub fn append_log_array(&mut self, log_array: MemLogArray) {
        self.logs.push(log_array);
    }

    /// Computes the contained quads based on the log entries.
    pub fn compute_quads(&self, until: VersionNumber) -> HashSet<EncodedQuad> {
        let mut quads = HashSet::new();

        for log_array in self.logs.iter() {
            if log_array.version_number > until {
                break;
            }

            for quad in &log_array.insertions() {
                quads.insert(quad.clone());
            }

            for quad in &log_array.deletions() {
                quads.remove(&quad);
            }
        }

        quads
    }
}

/// A [MemLogArray] contains the logs of a single transaction.
///
/// There are multiple types of log entries.
///
/// # Insert & Delete Entries
///
/// Each insert (delete) entry marks the insertion (deletion) of a single quad.
///
/// Each entry has four fields that correspond to the parts of the quad: graph, subject, predicate,
/// object.
pub struct MemLogArray {
    /// The version number of this transaction.
    pub version_number: VersionNumber,
    /// The union array that holds the actual log entries.
    pub array: Arc<UnionArray>,
}

impl MemLogArray {
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

/// Builder for [MemLogArray].
///
/// This struct only appends logs and does not check for their correctness. For example, when
/// inserting the caller must ensure that there are no duplicates.
pub struct MemLogArrayBuilder {
    /// The insertions.
    insertions: StructBuilder,
    /// The deletions.
    deletions: StructBuilder,
}

impl MemLogArrayBuilder {
    /// Creates a new [MemLogArrayBuilder].
    pub fn new() -> Self {
        Self {
            insertions: StructBuilder::from_fields(quad_fields(), 0),
            deletions: StructBuilder::from_fields(quad_fields(), 0),
        }
    }

    /// Appends a single quad to the insertion list.
    pub fn append_insertion(&mut self, quad: &EncodedQuad) {
        let graph = quad.graph_name.0.map(|oid| oid.as_object_id().0);
        let subject = quad.subject.as_object_id().0;
        let predicate = quad.predicate.as_object_id().0;
        let object = quad.object.as_object_id().0;

        self.insertions
            .field_builder::<UInt32Builder>(0)
            .expect("Schema fixed")
            .append_option(graph);

        self.insertions
            .field_builder::<UInt32Builder>(1)
            .expect("Schema fixed")
            .append_value(subject);

        self.insertions
            .field_builder::<UInt32Builder>(2)
            .expect("Schema fixed")
            .append_value(predicate);

        self.insertions
            .field_builder::<UInt32Builder>(3)
            .expect("Schema fixed")
            .append_value(object);

        self.insertions.append(true)
    }

    /// Builds the final array.
    pub fn build(
        mut self,
        version_number: VersionNumber,
    ) -> Result<MemLogArray, MemLogEntryBuildError> {
        let insertions = self.insertions.finish();
        let deletions = self.deletions.finish();

        let insertion_len = i32::try_from(insertions.len())?;
        let deletion_len = i32::try_from(deletions.len())?;
        let total_len = insertion_len as usize + deletion_len as usize;

        let mut type_ids: Vec<i8> = Vec::with_capacity(total_len);
        type_ids.extend(vec![0; insertion_len as usize]);
        type_ids.extend(vec![1; deletion_len as usize]);

        let mut offsets: Vec<i32> = Vec::with_capacity(total_len);
        offsets.extend(0..insertion_len);
        offsets.extend(0..deletion_len);

        let array = Arc::new(
            UnionArray::try_new(
                log_fields(),
                type_ids.into(),
                Some(offsets.into()),
                vec![Arc::new(insertions), Arc::new(deletions)],
            )
            .expect("Schema fixed"),
        );

        Ok(MemLogArray {
            version_number,
            array,
        })
    }
}

#[derive(Debug, Error)]
pub enum MemLogEntryBuildError {
    #[error("Too many log entries to fit into the arrow array.")]
    TooManyLogEntries,
}

impl From<TryFromIntError> for MemLogEntryBuildError {
    fn from(_: TryFromIntError) -> Self {
        Self::TooManyLogEntries
    }
}

impl From<MemLogEntryBuildError> for StorageError {
    fn from(value: MemLogEntryBuildError) -> Self {
        value.into()
    }
}

fn log_fields() -> UnionFields {
    UnionFields::new(
        [0i8, 1i8],
        [
            Field::new_struct("insertions", quad_fields(), false),
            Field::new_struct("deletions", quad_fields(), false),
        ],
    )
}

fn quad_fields() -> Vec<Field> {
    vec![
        Field::new(
            "graph_name",
            datafusion::arrow::datatypes::DataType::UInt32,
            true,
        ),
        Field::new(
            "subject",
            datafusion::arrow::datatypes::DataType::UInt32,
            false,
        ),
        Field::new(
            "predicate",
            datafusion::arrow::datatypes::DataType::UInt32,
            false,
        ),
        Field::new(
            "object",
            datafusion::arrow::datatypes::DataType::UInt32,
            false,
        ),
    ]
}
