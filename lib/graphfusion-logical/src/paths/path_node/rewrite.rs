use crate::paths::kleene_plus::KleenePlusPathNode;
use crate::paths::{COL_SOURCE, COL_TARGET};
use crate::{DFResult, PathNode, PatternNode};
use arrow_rdf::encoded::scalars::{encode_scalar_named_node, encode_scalar_predicate};
use arrow_rdf::encoded::{
    ENC_AS_NATIVE_BOOLEAN, ENC_EFFECTIVE_BOOLEAN_VALUE, ENC_SAME_TERM, ENC_WITH_SORTABLE_ENCODING,
};
use arrow_rdf::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT, TABLE_QUADS};
use datafusion::catalog::TableProvider;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::{Column, JoinType};
use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::{col, lit, Expr, Extension, LogicalPlan, LogicalPlanBuilder};
use datafusion::optimizer::{OptimizerConfig, OptimizerRule};
use datafusion::prelude::{not, or};
use oxrdf::NamedNode;
use spargebra::algebra::PropertyPathExpression;
use spargebra::term::NamedNodePattern;
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
        self.rewrite(plan)
    }
}

impl PathToJoinsRule {
    pub fn new(quads_table: Arc<dyn TableProvider>) -> Self {
        Self { quads_table }
    }

    fn rewrite(&self, plan: LogicalPlan) -> DFResult<Transformed<LogicalPlan>> {
        Ok(plan.transform(|plan| {
            let new_plan = match plan {
                LogicalPlan::Extension(Extension { node })
                    if node.as_any().downcast_ref::<PathNode>().is_some() =>
                {
                    let node = node.as_any().downcast_ref::<PathNode>().unwrap();

                    let query =
                        self.rewrite_property_path_expression(node.graph().as_ref(), node.path())?;
                    let graph_pattern =
                        node.graph().as_ref().map(|p| p.clone().into_term_pattern());

                    let pattern_node = match graph_pattern {
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
                    };

                    Transformed::yes(pattern_node.into())
                }
                _ => Transformed::no(plan),
            };
            Ok(new_plan)
        })?)
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
        let query = self
            .scan_quads(graph)?
            .filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_SAME_TERM.call(vec![
                col(COL_PREDICATE),
                lit(encode_scalar_predicate(node.as_ref())),
            ])]))?;

        query.project([
            col(COL_GRAPH),
            col(COL_SUBJECT).alias(COL_SOURCE),
            col(COL_OBJECT).alias(COL_TARGET),
        ])
    }

    /// Rewrites a negated property set to scanning the quads relation and checking whether the
    /// predicate does not match any of the given `nodes`.
    fn rewrite_negated_property_set(
        &self,
        graph: Option<&NamedNodePattern>,
        nodes: &Vec<NamedNode>,
    ) -> DFResult<LogicalPlanBuilder> {
        let test_expression = nodes
            .iter()
            .map(|nn| lit(encode_scalar_predicate(nn.as_ref())))
            .map(|expr| {
                ENC_EFFECTIVE_BOOLEAN_VALUE
                    .call(vec![ENC_SAME_TERM.call(vec![col(COL_PREDICATE), expr])])
            })
            .reduce(|lhs, rhs| or(lhs, rhs))
            .expect("There must be at least one element in the negated property set.");

        self.scan_quads(graph)?
            .filter(not(test_expression))?
            .project([
                col(COL_GRAPH),
                col(COL_SUBJECT).alias(COL_SOURCE),
                col(COL_OBJECT).alias(COL_TARGET),
            ])
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
            col(COL_SOURCE).alias(COL_TARGET),
            col(COL_TARGET).alias(COL_SOURCE),
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
        let lhs = self
            .rewrite_property_path_expression(graph, lhs)?
            .alias("lhs")?;
        let rhs = self
            .rewrite_property_path_expression(graph, rhs)?
            .alias("rhs")?;
        join_path_sequence(lhs, rhs)
    }

    /// Rewrites a zero or more to a CTE.
    fn rewrite_zero_or_more(
        &self,
        graph: Option<&NamedNodePattern>,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let zero = self.zero_length_paths(graph)?;
        let repetition = self.rewrite_one_or_more(graph, inner)?;
        join_path_alternatives(zero, repetition)
    }

    /// Rewrites a one or more by building a recursive query.
    fn rewrite_one_or_more(
        &self,
        graph: Option<&NamedNodePattern>,
        inner: &PropertyPathExpression,
    ) -> DFResult<LogicalPlanBuilder> {
        let inner = self.rewrite_property_path_expression(graph, inner)?;
        let node = KleenePlusPathNode::try_new(inner.build()?)?;

        let builder = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(node),
        }));
        Ok(builder.into())
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
        let subjects = self.scan_quads(graph)?.project([
            col(COL_GRAPH),
            col(COL_SUBJECT).alias(COL_SOURCE),
            col(COL_SUBJECT).alias(COL_TARGET),
        ])?;
        let objects = self.scan_quads(graph)?.project([
            col(COL_GRAPH),
            col(COL_OBJECT).alias(COL_SOURCE),
            col(COL_OBJECT).alias(COL_TARGET),
        ])?;
        subjects.union(objects.build()?)?.distinct_on(
            vec![
                ENC_WITH_SORTABLE_ENCODING.call(vec![col(COL_GRAPH)]),
                ENC_WITH_SORTABLE_ENCODING.call(vec![col(COL_SOURCE)]),
            ],
            vec![col(COL_GRAPH), col(COL_SOURCE), col(COL_TARGET)],
            None,
        )
    }

    /// Scans the quads table and optionally filters it to the given named graph.
    fn scan_quads(&self, graph: Option<&NamedNodePattern>) -> DFResult<LogicalPlanBuilder> {
        let query = LogicalPlanBuilder::scan(
            TABLE_QUADS,
            Arc::new(DefaultTableSource::new(Arc::clone(&self.quads_table))),
            None,
        )?;
        let query = match graph {
            Some(NamedNodePattern::NamedNode(nn)) => {
                query.filter(ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_SAME_TERM.call(vec![
                    col(COL_GRAPH),
                    lit(encode_scalar_named_node(nn.as_ref())),
                ])]))?
            }
            _ => query,
        };
        Ok(query)
    }
}

/// Creates a join that represents a sequence of two paths.
fn join_path_sequence(
    lhs: LogicalPlanBuilder,
    rhs: LogicalPlanBuilder,
) -> DFResult<LogicalPlanBuilder> {
    lhs.join_on(
        rhs.build()?,
        JoinType::Inner,
        [
            ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_SAME_TERM.call(vec![
                Expr::from(Column::new(Some("lhs"), COL_GRAPH)),
                Expr::from(Column::new(Some("rhs"), COL_GRAPH)),
            ])]),
            ENC_AS_NATIVE_BOOLEAN.call(vec![ENC_SAME_TERM.call(vec![
                Expr::from(Column::new(Some("lhs"), COL_TARGET)),
                Expr::from(Column::new(Some("rhs"), COL_SOURCE)),
            ])]),
        ],
    )?
    .project([
        col(Column::new(Some("lhs"), COL_GRAPH)),
        col(Column::new(Some("lhs"), COL_SOURCE)).alias(COL_SOURCE),
        col(Column::new(Some("rhs"), COL_TARGET)).alias(COL_TARGET),
    ])
}

/// Creates a union that represents an alternative of two paths.
fn join_path_alternatives(
    lhs: LogicalPlanBuilder,
    rhs: LogicalPlanBuilder,
) -> DFResult<LogicalPlanBuilder> {
    lhs.union(rhs.build()?)?.distinct_on(
        vec![
            ENC_WITH_SORTABLE_ENCODING.call(vec![col(COL_GRAPH)]),
            ENC_WITH_SORTABLE_ENCODING.call(vec![col(COL_SOURCE)]),
            ENC_WITH_SORTABLE_ENCODING.call(vec![col(COL_TARGET)]),
        ],
        vec![col(COL_GRAPH), col(COL_SOURCE), col(COL_TARGET)],
        None,
    )
}
