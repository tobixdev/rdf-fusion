mod log_insertion_stream;
mod pattern_stream;
mod quad_equalities;
mod quad_filter;

use crate::memory::encoding::{EncodedTermPattern, EncodedTriplePattern};
use datafusion::common::Column;
pub use log_insertion_stream::*;
pub use pattern_stream::*;
pub use quad_equalities::*;
use rdf_fusion_common::BlankNodeMatchingMode;
use rdf_fusion_model::{TermPattern, Variable};

/// Returns a buffer of optional variables from `graph_variable` and `pattern`.
fn extract_columns(
    graph_variable: Option<&Variable>,
    pattern: &EncodedTriplePattern,
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
    pattern: &EncodedTermPattern,
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
