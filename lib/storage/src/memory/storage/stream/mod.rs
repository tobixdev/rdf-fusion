mod log_insertion_stream;
mod pattern_stream;
mod quad_equalities;
mod quad_filter;

use crate::memory::MemObjectIdMapping;
use crate::memory::object_id::EncodedObjectId;
use datafusion::common::{Column, exec_datafusion_err};
pub use log_insertion_stream::*;
pub use pattern_stream::*;
use rdf_fusion_common::{BlankNodeMatchingMode, DFResult};
use rdf_fusion_model::{TermPattern, TriplePattern, Variable};

/// Returns a buffer of optional variables from `graph_variable` and `pattern`.
fn extract_columns(
    graph_variable: Option<&Variable>,
    pattern: &TriplePattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> [Option<Column>; 4] {
    [
        graph_variable
            .cloned()
            .map(|v| Column::new_unqualified(v.as_str())),
        extract_variable(&pattern.subject, blank_node_mode),
        extract_variable(&pattern.predicate.clone().into(), blank_node_mode),
        extract_variable(&pattern.object, blank_node_mode),
    ]
}

fn extract_variable(
    pattern: &TermPattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> Option<Column> {
    match pattern {
        TermPattern::Variable(v) => Some(Column::new_unqualified(v.as_str())),
        TermPattern::BlankNode(bnode)
            if blank_node_mode == BlankNodeMatchingMode::Variable =>
        {
            Some(Column::new_unqualified(bnode.as_str()))
        }
        _ => None,
    }
}

fn extract_term(
    object_id_mapping: &MemObjectIdMapping,
    pattern: &TermPattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> DFResult<Option<EncodedObjectId>> {
    match pattern {
        TermPattern::NamedNode(nn) => {
            let object_id = object_id_mapping
                .try_get_encoded_object_id_from_term(nn.as_ref())
                .ok_or(exec_datafusion_err!(
                    "Could not find object id. This check should happen during planning."
                ))?;
            Ok(Some(object_id))
        }
        TermPattern::BlankNode(bnode)
            if blank_node_mode == BlankNodeMatchingMode::Filter =>
        {
            let object_id = object_id_mapping
                .try_get_encoded_object_id_from_term(bnode.as_ref())
                .ok_or(exec_datafusion_err!(
                    "Could not find object id. This check should happen during planning."
                ))?;
            Ok(Some(object_id))
        }
        TermPattern::Literal(lit) => {
            let object_id = object_id_mapping
                .try_get_encoded_object_id_from_term(lit.as_ref())
                .ok_or(exec_datafusion_err!(
                    "Could not find object id. This check should happen during planning."
                ))?;
            Ok(Some(object_id))
        }
        _ => Ok(None),
    }
}
