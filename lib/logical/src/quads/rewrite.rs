use crate::quads::QuadsNode;
use crate::{DFResult, GraphFusionLogicalPlanBuilder};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{col, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use graphfusion_encoding::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use graphfusion_functions::registry::GraphFusionFunctionRegistryRef;
use graphfusion_model::TermRef;
use std::sync::Arc;

/// TODO
#[derive(Debug)]
pub struct QuadsToScanAndFilterRule {
    /// Used for creating expressions with GraphFusion builtins.
    registry: GraphFusionFunctionRegistryRef,
    // Reference to the registered Quads Table.
    quads_table: Arc<dyn TableProvider>,
}

impl OptimizerRule for QuadsToScanAndFilterRule {
    fn name(&self) -> &str {
        "quads_to_scan_and_filter"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<QuadsNode>() {
                        Transformed::yes(self.rewrite_quads_node(node)?)
                    } else {
                        Transformed::no(plan)
                    }
                }
                _ => Transformed::no(plan),
            };
            Ok(new_plan)
        })
    }
}

impl QuadsToScanAndFilterRule {
    /// TODO
    pub fn new(
        registry: GraphFusionFunctionRegistryRef,
        quads_table: Arc<dyn TableProvider>,
    ) -> Self {
        Self {
            registry,
            quads_table,
        }
    }

    /// TODO
    fn rewrite_quads_node(&self, node: &QuadsNode) -> DFResult<LogicalPlan> {
        let scan = LogicalPlanBuilder::scan(
            TABLE_QUADS,
            Arc::new(DefaultTableSource::new(Arc::clone(&self.quads_table))),
            None,
        )?;
        let plan =
            GraphFusionLogicalPlanBuilder::new(Arc::new(scan.build()?), Arc::clone(&self.registry));

        let active_graph_filter = plan
            .expr_builder()
            .filter_active_graph(col(COL_GRAPH), node.active_graph())?;
        let mut plan = plan.filter(active_graph_filter)?;

        if let Some(subject) = node.subject() {
            let filter = plan
                .expr_builder()
                .filter_by_scalar(col(COL_SUBJECT), TermRef::from(subject.as_ref()))?;
            plan = plan.filter(filter)?;
        }

        if let Some(predicate) = node.predicate() {
            let filter = plan
                .expr_builder()
                .filter_by_scalar(col(COL_PREDICATE), TermRef::from(predicate.as_ref()))?;
            plan = plan.filter(filter)?;
        }

        if let Some(predicate) = node.object() {
            let filter = plan
                .expr_builder()
                .filter_by_scalar(col(COL_OBJECT), predicate.as_ref())?;
            plan = plan.filter(filter)?;
        }

        // Project away bare table qualifiers
        plan.into_inner()
            .project(vec![
                col(COL_GRAPH).alias(COL_GRAPH),
                col(COL_SUBJECT).alias(COL_SUBJECT),
                col(COL_PREDICATE).alias(COL_PREDICATE),
                col(COL_OBJECT).alias(COL_OBJECT),
            ])?
            .build()
    }
}
