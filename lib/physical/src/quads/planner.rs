use crate::quads::QuadsExec;
use async_trait::async_trait;
use datafusion::error::Result as DFResult;
use datafusion::execution::context::SessionState;
use datafusion::logical_expr::{col, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::QuadPatternEvaluator;
use rdf_fusion_encoding::typed_value::DEFAULT_QUAD_DFSCHEMA;
use rdf_fusion_encoding::COL_GRAPH;
use rdf_fusion_functions::registry::RdfFusionFunctionRegistry;
use rdf_fusion_logical::quads::QuadsNode;
use rdf_fusion_logical::RdfFusionExprBuilderRoot;
use std::sync::Arc;

/// Planner for [QuadsNode].
pub struct QuadNodePlanner {
    /// TODO
    function_registry: Arc<dyn RdfFusionFunctionRegistry>,
    /// The implementation of the quad pattern evaluator.
    quad_pattern_evaluator: Arc<dyn QuadPatternEvaluator>,
}

impl QuadNodePlanner {
    /// Creates a new [QuadNodePlanner].
    pub fn new(
        function_registry: Arc<dyn RdfFusionFunctionRegistry>,
        quad_pattern_evaluator: Arc<dyn QuadPatternEvaluator>,
    ) -> Self {
        Self {
            function_registry,
            quad_pattern_evaluator,
        }
    }
}

#[async_trait]
impl ExtensionPlanner for QuadNodePlanner {
    /// Converts a logical [QuadsNode] into its physical execution plan
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(node) = node.as_any().downcast_ref::<QuadsNode>() {
            let expr_builder = RdfFusionExprBuilderRoot::new(
                self.function_registry.as_ref(),
                DEFAULT_QUAD_DFSCHEMA.as_ref(),
            );
            let filter_expr = expr_builder
                .try_create_builder(col(COL_GRAPH))?
                .build_filter_active_graph(node.active_graph())?
                .map(|e| {
                    planner.create_physical_expr(&e, DEFAULT_QUAD_DFSCHEMA.as_ref(), session_state)
                })
                .transpose()?;

            let quads = Arc::new(QuadsExec::new(
                Arc::clone(&self.quad_pattern_evaluator),
                node.active_graph().clone(),
                node.subject().cloned(),
                node.predicate().cloned(),
                node.object().cloned(),
            ));

            Ok(Some(match filter_expr {
                None => quads,
                Some(filter_expr) => Arc::new(FilterExec::try_new(filter_expr, quads)?),
            }))
        } else {
            Ok(None)
        }
    }
}
