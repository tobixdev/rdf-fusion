mod logical;
mod rewrite;

use crate::DFResult;
use datafusion::arrow::datatypes::{Field, Fields};
use datafusion::common::{DFSchema, DFSchemaRef};
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::TermEncoding;
pub use logical::*;
pub use rewrite::*;
use spargebra::term::TermPattern;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// TODO
pub fn compute_schema_for_pattern(patterns: &[Option<TermPattern>]) -> DFResult<DFSchemaRef> {
    let mut fields: HashSet<&str> = HashSet::new();
    for pattern in patterns {
        match pattern {
            Some(TermPattern::Variable(variable)) => {
                fields.insert(variable.as_str());
            }
            Some(TermPattern::BlankNode(bnode)) => {
                fields.insert(bnode.as_str());
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
