mod logical;
mod rewrite;

use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use logical::*;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::TermEncoding;
pub use rewrite::*;
use spargebra::term::TermPattern;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// TODO
pub fn compute_schema_for_pattern(patterns: &[Option<TermPattern>]) -> DFResult<DFSchemaRef> {
    let mut seen: HashSet<&str> = HashSet::new();
    let mut fields: Vec<&str> = Vec::new();

    for pattern in patterns {
        match pattern {
            Some(TermPattern::Variable(variable)) => {
                if !seen.contains(variable.as_str()) {
                    seen.insert(variable.as_str());
                    fields.push(variable.as_str());
                }
            }
            Some(TermPattern::BlankNode(bnode)) => {
                if !seen.contains(bnode.as_str()) {
                    seen.insert(bnode.as_str());
                    fields.push(bnode.as_str());
                }
            }
            _ => {}
        }
    }

    // TODO: base nullable on inner.
    let fields = fields
        .into_iter()
        .map(|name| Field::new(name, PlainTermEncoding::data_type(), true))
        .collect::<Fields>();
    Ok(Arc::new(DFSchema::from_unqualified_fields(
        fields,
        HashMap::new(),
    )?))
}
