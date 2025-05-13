use crate::expr_builder::RdfFusionExprBuilder;
use crate::paths::kleene_plus::KleenePlusClosureNode;
use crate::paths::{PropertyPathNode, COL_SOURCE, COL_TARGET, PATH_TABLE_DFSCHEMA};
use crate::patterns::PatternNode;
use crate::{ActiveGraph, DFResult, RdfFusionLogicalPlanBuilder};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{plan_datafusion_err, Column, JoinType};
use datafusion::logical_expr::{col, Expr, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::{not, or};
use graphfusion_encoding::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use graphfusion_functions::registry::RdfFusionFunctionRegistryRef;
use graphfusion_model::{NamedNode, TermRef};
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern, TriplePattern, Variable};
use std::sync::Arc;

#[derive(Debug)]
pub struct PropertyPathLoweringRule {
    /// Used for creating expressions with RdfFusion builtins.
    registry: RdfFusionFunctionRegistryRef,
}

impl OptimizerRule for PropertyPathLoweringRule {
    fn name(&self) -> &str {
        "property-path-lowering"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<PropertyPathNode>() {
                        Transformed::yes(self.rewrite_path_node(node)?)
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

impl PropertyPathLoweringRule {
    pub fn new(registry: RdfFusionFunctionRegistryRef) -> Self {
        Self { registry }
    }

    fn rewrite_path_node(&self, node: &PropertyPathNode) -> DFResult<LogicalPlan> {
        let query = self.rewrite_property_path_expression(node.active_graph(), node.path())?;

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(PatternNode::try_new(
                query.build()?,
                vec![
                    node.graph_name_var()
                        .map(|v| TermPattern::Variable(v.clone())),
                    node.subject().clone().into(),
                    node.object().clone().into(),
                ],
            )?),
        });
        Ok(logical_plan)
    }

    /// The resulting query always has a column "start" and "end" that indicates the respective start
    /// and end of the current path. In addition to that, the result contains a graph column and may
    /// contain additional fields that can bind values to variables.
    fn rewrite_property_path_expression(
        &self,
        graph: &ActiveGraph,
        path: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        match path {
            PropertyPathExpression::NamedNode(node) => self.rewrite_named_node(graph, node),
            PropertyPathExpression::Reverse(inner) => self.rewrite_reverse(graph, inner),
            PropertyPathExpression::Sequence(lhs, rhs) => self.rewrite_sequence(graph, lhs, rhs),
            PropertyPathExpression::Alternative(lhs, rhs) => {
                self.rewrite_alternative(graph, lhs, rhs)
            }
            PropertyPathExpression::ZeroOrMore(inner) => self.rewrite_zero_or_more(graph, inner),
            PropertyPathExpression::OneOrMore(inner) => self.rewrite_one_or_more(graph, inner),
            PropertyPathExpression::ZeroOrOne(inner) => self.rewrite_zero_or_one(graph, inner),
            PropertyPathExpression::NegatedPropertySet(inner) => {
                self.rewrite_negated_property_set(graph, inner)
            }
        }
    }

    /// Rewrites a named node path to scanning the quads relation and checking whether the predicate
    /// matches the given `node`.
    fn rewrite_named_node(
        &self,
        graph: &ActiveGraph,
        node: &NamedNode,
    ) -> DFResult<LogicalPlanBuilder> {
        let expr_builder =
            RdfFusionExprBuilder::new(&PATH_TABLE_DFSCHEMA, self.registry.as_ref());
        let filter =
            expr_builder.filter_by_scalar(col(COL_PREDICATE), TermRef::from(node.as_ref()))?;
        self.scan_quads(graph, Some(filter))
    }

    /// Rewrites a negated property set to scanning the quads relation and checking whether the
    /// predicate does not match any of the given `nodes`.
    fn rewrite_negated_property_set(
        &self,
        graph: &ActiveGraph,
        nodes: &[NamedNode],
    ) -> DFResult<LogicalPlanBuilder> {
        let expr_builder =
            RdfFusionExprBuilder::new(&PATH_TABLE_DFSCHEMA, self.registry.as_ref());
        let test_expressions = nodes
            .iter()
            .map(|nn| expr_builder.filter_by_scalar(col(COL_PREDICATE), TermRef::from(nn.as_ref())))
            .collect::<DFResult<Vec<Expr>>>()?;
        let test_expression =
            test_expressions
                .into_iter()
                .reduce(or)
                .ok_or(plan_datafusion_err!(
                    "The negated property set must not be empty"
                ))?;

        self.scan_quads(graph, Some(not(test_expression)))?
            .distinct()
    }

    /// Reverses the inner path by swapping [COL_SOURCE] and [COL_TARGET].
    fn rewrite_reverse(
        &self,
        graph: &ActiveGraph,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_property_path_expression(graph, inner)?;
        inner.project([
            col(COL_GRAPH),
            col(COL_TARGET).alias(COL_SOURCE),
            col(COL_SOURCE).alias(COL_TARGET),
        ])
    }

    /// Rewrites an alternative path to union over both (distinct).
    fn rewrite_alternative(
        &self,
        graph: &ActiveGraph,
        lhs: &PropertyPathExpression,
        rhs: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_property_path_expression(graph, lhs)?;
        let rhs = self.rewrite_property_path_expression(graph, rhs)?;
        self.join_path_alternatives(lhs, rhs)?.distinct()
    }

    /// Rewrites a sequence by joining the [COL_TARGET] of the lhs to the [COL_SOURCE] of the `rhs`.
    fn rewrite_sequence(
        &self,
        graph: &ActiveGraph,
        lhs: &PropertyPathExpression,
        rhs: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_property_path_expression(graph, lhs)?;
        let rhs = self.rewrite_property_path_expression(graph, rhs)?;
        self.join_path_sequence(graph, lhs, rhs)?.distinct()
    }

    /// Rewrites a zero or more to a CTE.
    fn rewrite_zero_or_more(
        &self,
        graph: &ActiveGraph,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let zero = self.zero_length_paths(graph)?;
        let repetition = self.rewrite_one_or_more(graph, inner)?;
        self.join_path_alternatives(zero, repetition)?.distinct()
    }

    /// Rewrites a one or more by building a recursive query.
    fn rewrite_one_or_more(
        &self,
        graph: &ActiveGraph,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_property_path_expression(graph, inner)?;
        let allow_cross_graph_paths = matches!(graph, ActiveGraph::DefaultGraph);
        let node = KleenePlusClosureNode::try_new(inner.build()?, allow_cross_graph_paths)?;

        let builder = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }));
        Ok(builder)
    }

    fn rewrite_zero_or_one(
        &self,
        graph: &ActiveGraph,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let zero = self.zero_length_paths(graph)?;
        let one = self.rewrite_property_path_expression(graph, inner)?;
        self.join_path_alternatives(zero, one)
    }

    /// Returns a list of all subjects and objects in the graph where they both are the source and
    /// the target of the path.
    fn zero_length_paths(&self, graph: &ActiveGraph) -> DFResult<LogicalPlanBuilder> {
        // TODO: This must be optimized
        let subjects = self.scan_quads(graph, None)?.project([
            col(COL_GRAPH).alias(COL_GRAPH),
            col(COL_SOURCE).alias(COL_SOURCE),
            col(COL_SOURCE).alias(COL_TARGET),
        ])?;
        let objects = self.scan_quads(graph, None)?.project([
            col(COL_GRAPH).alias(COL_GRAPH),
            col(COL_TARGET).alias(COL_SOURCE),
            col(COL_TARGET).alias(COL_TARGET),
        ])?;
        subjects.union(objects.build()?)?.distinct()
    }

    /// Creates a join that represents a sequence of two paths.
    fn join_path_sequence(
        &self,
        active_graph: &ActiveGraph,
        lhs: LogicalPlanBuilder,
        rhs: LogicalPlanBuilder,
    ) -> DFResult<LogicalPlanBuilder> {
        let expr_builder =
            RdfFusionExprBuilder::new(&PATH_TABLE_DFSCHEMA, self.registry.as_ref());
        let path_join_expr = expr_builder.same_term(
            Expr::from(Column::new(Some("lhs"), COL_TARGET)),
            Expr::from(Column::new(Some("rhs"), COL_SOURCE)),
        )?;
        let mut on_exprs = vec![path_join_expr];

        if !allows_cross_graph_paths(active_graph) {
            let graph_expr = expr_builder.same_term(
                Expr::from(Column::new(Some("lhs"), COL_GRAPH)),
                Expr::from(Column::new(Some("rhs"), COL_GRAPH)),
            )?;
            on_exprs.push(graph_expr)
        }

        lhs.alias("lhs")?
            .join_on(rhs.alias("rhs")?.build()?, JoinType::Inner, on_exprs)?
            .project([
                col(Column::new(Some("lhs"), COL_GRAPH)).alias(COL_GRAPH),
                col(Column::new(Some("lhs"), COL_SOURCE)).alias(COL_SOURCE),
                col(Column::new(Some("rhs"), COL_TARGET)).alias(COL_TARGET),
            ])
    }

    /// Creates a union that represents an alternative of two paths.
    fn join_path_alternatives(
        &self,
        lhs: LogicalPlanBuilder,
        rhs: LogicalPlanBuilder,
    ) -> DFResult<LogicalPlanBuilder> {
        lhs.union(rhs.build()?)
    }

    /// Scans the quads table and optionally filters it.
    fn scan_quads(
        &self,
        active_graph: &ActiveGraph,
        filter: Option<Expr>,
    ) -> DFResult<LogicalPlanBuilder> {
        let pattern = TriplePattern {
            subject: TermPattern::Variable(Variable::new_unchecked(COL_SUBJECT)),
            predicate: NamedNodePattern::Variable(Variable::new_unchecked(COL_PREDICATE)),
            object: TermPattern::Variable(Variable::new_unchecked(COL_TARGET)),
        };
        let builder = RdfFusionLogicalPlanBuilder::new_from_pattern(
            Arc::clone(&self.registry),
            active_graph.clone(),
            Some(Variable::new_unchecked(COL_GRAPH)),
            pattern,
        )?;

        // Apply filter if present
        let builder = if let Some(filter) = filter {
            builder.filter(filter)?
        } else {
            builder
        };

        // Project columns to PATH_TABLE
        let query = builder.into_inner().project([
            col(COL_GRAPH).alias(COL_GRAPH),
            col(COL_SUBJECT).alias(COL_SOURCE),
            col(COL_OBJECT).alias(COL_TARGET),
        ])?;

        Ok(query)
    }
}

/// TODO
fn allows_cross_graph_paths(graph: &ActiveGraph) -> bool {
    matches!(graph, ActiveGraph::DefaultGraph)
}
