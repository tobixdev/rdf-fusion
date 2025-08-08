use crate::memory::encoding::EncodedQuad;
use crate::memory::storage::log::VersionNumber;
use crate::memory::storage::log::content::{
    MemLogEntry, MemLogEntryAction, MemLogUpdateArray,
};
use datafusion::arrow::array::{Array, StructBuilder, UInt32Builder, UnionArray};
use datafusion::arrow::datatypes::{Field, UnionFields};
use rdf_fusion_common::error::StorageError;
use std::num::TryFromIntError;
use std::sync::Arc;
use thiserror::Error;

/// Builder for [MemLogUpdateArray].
///
/// This struct only appends logs and does not check for their correctness. For example, when
/// inserting the caller must ensure that there are no duplicates.
pub struct MemLogEntryBuilder {
    /// There are only a few operations that can be bundled together in a single transaction. The
    /// state encapsulates the incompatibility between operations. For example, inserting quads into
    /// the store and then calling [Self::clear] will trigger an error.
    state: Option<MemLogArrayBuilderState>,
}

/// The state of the builder.
enum MemLogArrayBuilderState {
    /// The user is building an update.
    Update(MemLogUpdateBuilderState),
    /// A final action.
    Action(MemLogEntryAction),
}

/// Represents the state of adding and deleting quads.
struct MemLogUpdateBuilderState {
    /// The insertions.
    insertions: StructBuilder,
    /// The deletions.
    deletions: StructBuilder,
}

impl MemLogEntryBuilder {
    /// Creates a new [MemLogEntryBuilder].
    pub fn new() -> Self {
        Self { state: None }
    }

    /// Ensures that the current state is [MemLogArrayBuilderState::Update].
    fn ensure_update(&mut self) -> Result<(), MemLogEntryBuilderError> {
        if let Some(MemLogArrayBuilderState::Action(_)) = &self.state {
            return Err(MemLogEntryBuilderError::InvalidState);
        }

        if let Some(MemLogArrayBuilderState::Update(_)) = &self.state {
            return Ok(());
        }

        let state = MemLogUpdateBuilderState {
            insertions: StructBuilder::from_fields(quad_fields(), 0),
            deletions: StructBuilder::from_fields(quad_fields(), 0),
        };
        self.state = Some(MemLogArrayBuilderState::Update(state));
        Ok(())
    }

    /// Returns a reference to the update builder state, if available.
    fn update_state_mut(&mut self) -> Option<&mut MemLogUpdateBuilderState> {
        match &mut self.state {
            Some(MemLogArrayBuilderState::Update(state)) => Some(state),
            _ => None,
        }
    }

    /// Explicitly sets the current state to `state`. Returns an error if a state is already set.
    fn set_action(
        &mut self,
        state: MemLogArrayBuilderState,
    ) -> Result<(), MemLogEntryBuilderError> {
        match self.state {
            None => {
                self.state = Some(state);
                Ok(())
            }
            Some(_) => Err(MemLogEntryBuilderError::InvalidState),
        }
    }

    /// Appends a single quad to the insertion list.
    pub fn append_insertion(
        &mut self,
        quad: &EncodedQuad,
    ) -> Result<(), MemLogEntryBuilderError> {
        self.ensure_update()?;
        let state = self.update_state_mut().expect("State set");
        append_quad(&mut state.insertions, quad)
    }

    /// Appends a single quad to the insertion list.
    pub fn append_deletion(
        &mut self,
        quad: &EncodedQuad,
    ) -> Result<(), MemLogEntryBuilderError> {
        self.ensure_update()?;
        let state = self.update_state_mut().expect("State set");
        append_quad(&mut state.deletions, quad)
    }

    /// Declares that this transaction will execute the given action.
    pub fn action(
        &mut self,
        action: MemLogEntryAction,
    ) -> Result<(), MemLogEntryBuilderError> {
        self.set_action(MemLogArrayBuilderState::Action(action))
    }

    /// Builds the final array.
    pub fn build(
        self,
        version_number: VersionNumber,
    ) -> Result<Option<MemLogEntry>, MemLogEntryBuildError> {
        let action = match self.state {
            None => {
                return Ok(None);
            }
            Some(MemLogArrayBuilderState::Update(state)) => {
                Self::build_update_action(state)?
            }
            Some(MemLogArrayBuilderState::Action(action)) => action,
        };

        Ok(Some(MemLogEntry {
            action,
            version_number,
        }))
    }

    /// Builds an [MemLogEntryAction::Update].
    fn build_update_action(
        mut state: MemLogUpdateBuilderState,
    ) -> Result<MemLogEntryAction, MemLogEntryBuildError> {
        let insertions = state.insertions.finish();
        let deletions = state.deletions.finish();

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

        Ok(MemLogEntryAction::Update(MemLogUpdateArray { array }))
    }
}

/// Appends a single quad to the insertion list.
pub fn append_quad(
    builder: &mut StructBuilder,
    quad: &EncodedQuad,
) -> Result<(), MemLogEntryBuilderError> {
    let graph = quad.graph_name.0.map(|oid| oid.as_object_id().0);
    let subject = quad.subject.as_object_id().0;
    let predicate = quad.predicate.as_object_id().0;
    let object = quad.object.as_object_id().0;

    builder
        .field_builder::<UInt32Builder>(0)
        .expect("Schema fixed")
        .append_option(graph);

    builder
        .field_builder::<UInt32Builder>(1)
        .expect("Schema fixed")
        .append_value(subject);

    builder
        .field_builder::<UInt32Builder>(2)
        .expect("Schema fixed")
        .append_value(predicate);

    builder
        .field_builder::<UInt32Builder>(3)
        .expect("Schema fixed")
        .append_value(object);

    builder.append(true);

    Ok(())
}

#[derive(Debug, Error)]
pub enum MemLogEntryBuildError {
    #[error("Too many log entries to fit into the arrow array.")]
    TooManyLogEntries,
    #[error("The transaction did not contain any actions.")]
    NoAction,
}

#[derive(Debug, Error)]
pub enum MemLogEntryBuilderError {
    #[error("Invalid state for action builder.")]
    InvalidState,
}

impl From<MemLogEntryBuilderError> for StorageError {
    fn from(value: MemLogEntryBuilderError) -> Self {
        StorageError::Other(Box::new(value))
    }
}

impl From<TryFromIntError> for MemLogEntryBuildError {
    fn from(_: TryFromIntError) -> Self {
        Self::TooManyLogEntries
    }
}

impl From<MemLogEntryBuildError> for StorageError {
    fn from(value: MemLogEntryBuildError) -> Self {
        StorageError::Other(Box::new(value))
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
