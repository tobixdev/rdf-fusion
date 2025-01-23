use crate::results::decoding::logical::DecodeRdfTermsNode;
use crate::DFResult;
use arrow_rdf::encoded::EncTerm;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::logical_expr::{Extension, LogicalPlan};
use std::sync::Arc;

mod logical;
mod rewrite;

pub use rewrite::DecodeRdfTermsToProjectionRule;

pub fn decode_rdf_terms(input: LogicalPlan) -> DFResult<LogicalPlan> {
    Ok(LogicalPlan::Extension(Extension {
        node: Arc::new(DecodeRdfTermsNode::new(input)?),
    }))
}

fn compute_decoded_schema(input_schema: &Schema) -> SchemaRef {
    let fields: Vec<_> = input_schema
        .fields()
        .iter()
        .map(|f| transform_field(f))
        .collect();
    SchemaRef::new(Schema::new(fields))
}

fn transform_field(field: &Field) -> Field {
    if *field.data_type() == EncTerm::term_type() {
        Field::new(field.name(), EncTerm::term_type(), field.is_nullable())
    } else {
        field.clone()
    }
}
