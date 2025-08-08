//! SPARQL logical query plan.
//!
//! This crate contains the building blocks for creating a SPARQL logical query plan
//! that can be optimized and executed by Apache DataFusion. It provides builders
//! for constructing the plan programmatically and defines custom logical nodes
//! for SPARQL-specific operations.

extern crate core;

mod active_graph;
pub mod expr;
mod expr_builder;
mod expr_builder_context;
pub mod extend;
pub mod join;
mod logical_plan_builder;
mod logical_plan_builder_context;
pub mod minus;
pub mod paths;
pub mod patterns;
pub mod quad_pattern;

pub use active_graph::{ActiveGraph, EnumeratedActiveGraph};
use datafusion::common::{plan_err, DFSchema};
pub use expr_builder::RdfFusionExprBuilder;
pub use expr_builder_context::RdfFusionExprBuilderContext;
pub use logical_plan_builder::RdfFusionLogicalPlanBuilder;
pub use logical_plan_builder_context::RdfFusionLogicalPlanBuilderContext;
use rdf_fusion_common::DFResult;

/// Checks if two schemas are logically equivalent in terms of names and types.
pub(crate) fn check_same_schema(
    old_schema: &DFSchema,
    new_schema: &DFSchema,
) -> DFResult<()> {
    if !old_schema.logically_equivalent_names_and_types(new_schema) {
        return plan_err!(
            "Schema of the new plan is not compatible with the old one. Old Schema: {:?}. New Schema: {:?}",
            old_schema,
            new_schema
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rdf_fusion_api::RdfFusionContextView;
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
    use rdf_fusion_encoding::typed_value::TYPED_VALUE_ENCODING;
    use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings};
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
    use std::sync::Arc;

    pub(crate) fn create_test_context() -> RdfFusionContextView {
        let encodings = RdfFusionEncodings::new(
            PLAIN_TERM_ENCODING,
            TYPED_VALUE_ENCODING,
            None,
            SORTABLE_TERM_ENCODING,
        );
        RdfFusionContextView::new(
            Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone())),
            encodings,
            QuadStorageEncoding::PlainTerm,
        )
    }
}
