use crate::paths::kleene_plus::KleenePlusClosureNode;
use crate::paths::{PathNode, COL_SOURCE, COL_TARGET};
use crate::patterns::PatternNode;
use crate::DFResult;
use arrow_rdf::encoded::scalars::{encode_scalar_named_node, encode_scalar_predicate};
use arrow_rdf::encoded::{
    ENC_AS_NATIVE_BOOLEAN, ENC_EFFECTIVE_BOOLEAN_VALUE, ENC_IS_COMPATIBLE, ENC_SAME_TERM,
    ENC_WITH_SORTABLE_ENCODING,
};
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{plan_datafusion_err, Column, JoinType};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{col, lit, Expr, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::{not, or};
use model::NamedNode;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::{NamedNodePattern, TermPattern};
use std::sync::Arc;

#[derive(Debug)]
pub struct PathToJoinsRule {
    // TODO: Check if we can remove this and just use TABLE_QUADS in the logical plan
    quads_table: Arc<dyn TableProvider>,
}

impl OptimizerRule for PathToJoinsRule {
    fn name(&self) -> &str {
        "path_to_joins_rule"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> DFResult<Transformed<LogicalPlan>> {
        plan.transform(|plan| {
            let new_plan = match &plan {
                LogicalPlan::Extension(Extension { node }) => {
                    if let Some(node) = node.as_any().downcast_ref::<PathNode>() {
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

impl PathToJoinsRule {
    pub fn new(quads_table: Arc<dyn TableProvider>) -> Self {
        Self { quads_table }
    }

    fn rewrite_path_node(&self, node: &PathNode) -> DFResult<LogicalPlan> {
        let query = self.rewrite_property_path_expression(node.graph().as_ref(), node.path())?;
        let graph_pattern = node.graph().as_ref().map(|p| TermPattern::from(p.clone()));

        Ok(match graph_pattern {
            None => LogicalPlan::Extension(Extension {
                node: Arc::new(PatternNode::try_new(
                    query.project([col(COL_SOURCE), col(COL_TARGET)])?.build()?,
                    vec![node.subject().clone(), node.object().clone()],
                )?),
            }),
            Some(graph_pattern) => LogicalPlan::Extension(Extension {
                node: Arc::new(PatternNode::try_new(
                    query.build()?,
                    vec![graph_pattern, node.subject().clone(), node.object().clone()],
                )?),
            }),
        })
    }

    /// The resulting query always has a column "start" and "end" that indicates the respective start
    /// and end of the current path. In addition to that, the result contains a graph column and may
    /// contain additional fields that can bind values to variables.
    fn rewrite_property_path_expression(
        &self,
        graph: Option<&NamedNodePattern>,
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
        graph: Option<&NamedNodePattern>,
        node: &NamedNode,
    ) -> DFResult<LogicalPlanBuilder> {
        let filter = ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_SAME_TERM.call(vec![
            col(COL_PREDICATE),
            lit(encode_scalar_predicate(node.as_ref())),
        ])]);
        self.scan_quads(graph, Some(filter))
    }

    /// Rewrites a negated property set to scanning the quads relation and checking whether the
    /// predicate does not match any of the given `nodes`.
    fn rewrite_negated_property_set(
        &self,
        graph: Option<&NamedNodePattern>,
        nodes: &[NamedNode],
    ) -> DFResult<LogicalPlanBuilder> {
        let test_expression = nodes
            .iter()
            .map(|nn| lit(encode_scalar_predicate(nn.as_ref())))
            .map(|expr| {
                ENC_EFFECTIVE_BOOLEAN_VALUE
                    .call(vec![ENC_SAME_TERM.call(vec![col(COL_PREDICATE), expr])])
            })
            .reduce(or)
            .ok_or(plan_datafusion_err!(
                "The negated property set must not be empty"
            ))?;

        let paths = self.scan_quads(graph, Some(not(test_expression)))?;
        make_distinct(paths)
    }

    /// Reverses the inner path by swapping [COL_SOURCE] and [COL_TARGET].
    fn rewrite_reverse(
        &self,
        graph: Option<&NamedNodePattern>,
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
        graph: Option<&NamedNodePattern>,
        lhs: &PropertyPathExpression,
        rhs: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_property_path_expression(graph, lhs)?;
        let rhs = self.rewrite_property_path_expression(graph, rhs)?;
        join_path_alternatives(lhs, rhs)
    }

    /// Rewrites a sequence by joining the [COL_TARGET] of the lhs to the [COL_SOURCE] of the `rhs`.
    fn rewrite_sequence(
        &self,
        graph: Option<&NamedNodePattern>,
        lhs: &PropertyPathExpression,
        rhs: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let lhs = self.rewrite_property_path_expression(graph, lhs)?;
        let rhs = self.rewrite_property_path_expression(graph, rhs)?;
        make_distinct(join_path_sequence(graph, lhs, rhs)?)
    }

    /// Rewrites a zero or more to a CTE.
    fn rewrite_zero_or_more(
        &self,
        graph: Option<&NamedNodePattern>,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let zero = self.zero_length_paths(graph)?;
        let repetition = self.rewrite_one_or_more(graph, inner)?;
        let paths = join_path_alternatives(zero, repetition)?;
        make_distinct(paths)
    }

    /// Rewrites a one or more by building a recursive query.
    fn rewrite_one_or_more(
        &self,
        graph: Option<&NamedNodePattern>,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_property_path_expression(graph, inner)?;
        let allow_cross_graph_paths = graph.is_none();
        let node = KleenePlusClosureNode::try_new(inner.build()?, allow_cross_graph_paths)?;

        let builder = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }));
        Ok(builder)
    }

    fn rewrite_zero_or_one(
        &self,
        graph: Option<&NamedNodePattern>,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let zero = self.zero_length_paths(graph)?;
        let one = self.rewrite_property_path_expression(graph, inner)?;
        join_path_alternatives(zero, one)
    }

    /// Returns a list of all subjects and objects in the graph where they both are the source and
    /// the target of the path.
    fn zero_length_paths(&self, graph: Option<&NamedNodePattern>) -> DFResult<LogicalPlanBuilder> {
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
        make_distinct(subjects.union(objects.build()?)?)
    }

    /// Scans the quads table and optionally filters it to the given named graph.
    fn scan_quads(
        &self,
        graph: Option<&NamedNodePattern>,
        filter: Option<Expr>,
    ) -> DFResult<LogicalPlanBuilder> {
        let query = LogicalPlanBuilder::scan(
            TABLE_QUADS,
            Arc::new(DefaultTableSource::new(Arc::clone(&self.quads_table))),
            None,
        )?;

        // Apply graph filter if present
        let query = match graph {
            Some(NamedNodePattern::NamedNode(nn)) => {
                query.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_SAME_TERM.call(vec![
                    col(COL_GRAPH),
                    lit(encode_scalar_named_node(nn.as_ref())),
                ])]))?
            }
            _ => query,
        };

        // Apply filter if present
        let query = if let Some(filter) = filter {
            query.filter(filter)?
        } else {
            query
        };

        // Project columns to PATH_TABLE
        let query = query.project([
            col(COL_GRAPH).alias(COL_GRAPH),
            col(COL_SUBJECT).alias(COL_SOURCE),
            col(COL_OBJECT).alias(COL_TARGET),
        ])?;

        Ok(query)
    }
}

/// Creates a join that represents a sequence of two paths.
fn join_path_sequence(
    graph: Option<&NamedNodePattern>,
    lhs: LogicalPlanBuilder,
    rhs: LogicalPlanBuilder,
) -> DFResult<LogicalPlanBuilder> {
    let path_join_expr = ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_SAME_TERM.call(vec![
        Expr::from(Column::new(Some("lhs"), COL_TARGET)),
        Expr::from(Column::new(Some("rhs"), COL_SOURCE)),
    ])]);
    let mut on_exprs = vec![path_join_expr];

    if graph.is_some() {
        on_exprs.push(ENC_IS_COMPATIBLE.call(vec![
            Expr::from(Column::new(Some("lhs"), COL_GRAPH)),
            Expr::from(Column::new(Some("rhs"), COL_GRAPH)),
        ]))
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
    lhs: LogicalPlanBuilder,
    rhs: LogicalPlanBuilder,
) -> DFResult<LogicalPlanBuilder> {
    lhs.union(rhs.build()?)
}

/// Makes the path set distinct.
fn make_distinct(builder: LogicalPlanBuilder) -> DFResult<LogicalPlanBuilder> {
    builder.distinct_on(
        vec![
            ENC_WITH_SORTABLE_ENCODING.call(vec![col(COL_GRAPH)]),
            ENC_WITH_SORTABLE_ENCODING.call(vec![col(COL_SOURCE)]),
            ENC_WITH_SORTABLE_ENCODING.call(vec![col(COL_TARGET)]),
        ],
        vec![col(COL_GRAPH), col(COL_SOURCE), col(COL_TARGET)],
        None,
    )
}
