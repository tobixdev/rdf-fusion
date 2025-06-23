mod logical;
mod rewrite;

use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use logical::*;
use rdf_fusion_common::BlankNodeMatchingMode;
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_DFSCHEMA;
use rdf_fusion_model::{TermPattern, TriplePattern, VariableRef};
pub use rewrite::*;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// TODO
pub fn compute_schema_for_triple_pattern(
    graph_variable: Option<VariableRef<'_>>,
    pattern: &TriplePattern,
    blank_node_mode: BlankNodeMatchingMode,
) -> DFSchemaRef {
    compute_schema_for_pattern(
        &DEFAULT_QUAD_DFSCHEMA,
        &vec![
            graph_variable
                .as_ref()
                .map(|v| TermPattern::Variable(v.into_owned())),
            Some(pattern.subject.clone()),
            Some(pattern.predicate.clone().into()),
            Some(pattern.object.clone()),
        ],
        blank_node_mode,
    )
}

/// TODO
#[allow(clippy::expect_used, reason = "Variables should not clash")]
pub fn compute_schema_for_pattern(
    inner_schema: &DFSchema,
    patterns: &[Option<TermPattern>],
    blank_node_mode: BlankNodeMatchingMode,
) -> DFSchemaRef {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut fields: Vec<(&str, &Field)> = Vec::new();

    for (pattern, field) in patterns.iter().zip(inner_schema.fields()) {
        match pattern {
            Some(TermPattern::Variable(variable)) => {
                if !seen.contains(variable.as_str()) {
                    seen.insert(variable.as_str());
                    fields.push((variable.as_str(), field));
                }
            }
            // A blank node only leads to an output variable if it is matched like a variable
            Some(TermPattern::BlankNode(bnode))
                if blank_node_mode == BlankNodeMatchingMode::Variable =>
            {
                if !seen.contains(bnode.as_str()) {
                    seen.insert(bnode.as_str());
                    fields.push((bnode.as_str(), field));
                }
            }
            _ => {}
        }
    }

    let fields = fields
        .into_iter()
        .map(|(name, field)| Field::new(name, field.data_type().clone(), field.is_nullable()))
        .collect::<Fields>();
    Arc::new(
        DFSchema::from_unqualified_fields(fields, HashMap::new())
            .expect("Fields already deduplicated."),
    )
}
