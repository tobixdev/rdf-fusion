mod logical;
mod rewrite;

use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use logical::*;
use rdf_fusion_common::DFResult;
pub use rewrite::*;
use spargebra::term::TermPattern;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// TODO
pub fn compute_schema_for_pattern(
    inner_schema: &DFSchema,
    patterns: &[Option<TermPattern>],
) -> DFResult<DFSchemaRef> {
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
            Some(TermPattern::BlankNode(bnode)) => {
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
    Ok(Arc::new(DFSchema::from_unqualified_fields(
        fields,
        HashMap::new(),
    )?))
}
