//! SPARQL logical query plan.
//!
//! This crate contains the building blocks for creating a SPARQL logical query plan
//! that can be optimized and executed by Apache DataFusion. It provides builders
//! for constructing the plan programmatically and defines custom logical nodes
//! for SPARQL-specific operations.

extern crate core;

mod active_graph;
mod expr_builder;
mod expr_builder_root;
pub mod extend;
pub mod join;
mod logical_plan_builder;
pub mod minus;
pub mod paths;
pub mod patterns;
pub mod quad_pattern;

pub use active_graph::{ActiveGraph, EnumeratedActiveGraph};
use datafusion::common::{plan_err, DFSchema};
pub use expr_builder::RdfFusionExprBuilder;
pub use expr_builder_root::RdfFusionExprBuilderRoot;
pub use logical_plan_builder::RdfFusionLogicalPlanBuilder;
use rdf_fusion_common::DFResult;

/// Checks if two schemas are logically equivalent in terms of names and types.
pub(crate) fn check_same_schema(old_schema: &DFSchema, new_schema: &DFSchema) -> DFResult<()> {
    if !old_schema.logically_equivalent_names_and_types(new_schema) {
        return plan_err!(
            "Schema of the new plan is not compatible with the old one. Old Schema: {:?}. New Schema: {:?}",
            old_schema,
            new_schema
        );
    }
    Ok(())
}
