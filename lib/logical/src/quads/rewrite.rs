use crate::quads::QuadsNode;
use crate::{check_same_schema, RdfFusionLogicalPlanBuilder};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{
    col, Extension, LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNodeCore,
};
use datafusion::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use rdf_fusion_encoding::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use rdf_fusion_functions::registry::RdfFusionFunctionRegistryRef;
use rdf_fusion_model::TermRef;
use std::sync::Arc;
use rdf_fusion_common::DFResult;

/// Lowers a [QuadsNode] into a full scan of the quads table and a [PatternNode] that then filters
/// and projects this result.
///
/// This can be used to support quad tables that do not have native support for evaluating quad
/// patterns.
///
/// ### Performance
///
/// This rewriting rule will only exhibit good performance if the quad table provider supports a
/// generic version of filter and predicate pushdowns. Otherwise, *every* quad pattern will require
/// a full scan of the quads table, seriously affecting the performance of the query. This is the
/// reason why this rewriting rule is not enabled by default. Use it with caution or for
/// experiments.
#[derive(Debug)]
pub struct QuadsLoweringRule {
    /// Used for creating expressions with RdfFusion builtins.
    registry: RdfFusionFunctionRegistryRef,
    /// Reference to the registered Quads Table.
    quads_table: Arc<dyn TableProvider>,
}

impl OptimizerRule for QuadsLoweringRule {
    fn name(&self) -> &str {
        "quads-lowering"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
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
                        let new_plan = self.rewrite_quads_node(node)?;
                        check_same_schema(node.schema(), new_plan.schema())?;
                        Transformed::yes(new_plan)
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

impl QuadsLoweringRule {
    /// TODO
    pub fn new(
        registry: RdfFusionFunctionRegistryRef,
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
        let mut plan =
            RdfFusionLogicalPlanBuilder::new(Arc::new(scan.build()?), Arc::clone(&self.registry));

        let active_graph_filter = plan
            .expr_builder(col(COL_GRAPH))?
            .build_filter_active_graph(node.active_graph())?;
        if let Some(active_graph_filter) = active_graph_filter {
            plan = plan.filter(active_graph_filter)?;
        }

        if let Some(subject) = node.subject() {
            let filter = plan
                .expr_builder(col(COL_SUBJECT))?
                .build_same_term_scalar(TermRef::from(subject.as_ref()))?;
            plan = plan.filter(filter)?;
        }

        if let Some(predicate) = node.predicate() {
            let filter = plan
                .expr_builder(col(COL_PREDICATE))?
                .build_same_term_scalar(TermRef::from(predicate.as_ref()))?;
            plan = plan.filter(filter)?;
        }

        if let Some(predicate) = node.object() {
            let filter = plan
                .expr_builder(col(COL_OBJECT))?
                .build_same_term_scalar(predicate.as_ref())?;
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
