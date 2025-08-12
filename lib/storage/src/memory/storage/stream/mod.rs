mod pattern_stream;
mod quad_equalities;

use crate::memory::encoding::EncodedTriplePattern;
use datafusion::common::Column;
pub use pattern_stream::*;
pub use quad_equalities::*;
use rdf_fusion_model::Variable;

/// Returns a buffer of optional variables from `graph_variable` and `pattern`.
fn extract_columns(
    graph_variable: Option<&Variable>,
    pattern: &EncodedTriplePattern,
) -> [Option<Column>; 4] {
    [
        graph_variable
            .cloned()
            .map(|v| Column::new_unqualified(v.as_str())),
        pattern
            .subject
            .try_as_variable()
            .map(Column::new_unqualified),
        pattern
            .predicate
            .try_as_variable()
            .map(Column::new_unqualified),
        pattern
            .object
            .try_as_variable()
            .map(Column::new_unqualified),
    ]
}
